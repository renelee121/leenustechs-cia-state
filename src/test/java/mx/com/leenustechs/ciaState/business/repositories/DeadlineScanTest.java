package mx.com.leenustechs.ciaState.business.repositories;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.Query;
import com.google.firebase.database.ValueEventListener;

import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;
import mx.com.leenustechs.ciaState.models.records.DeadlineCursor;
import mx.com.leenustechs.ciaState.models.records.WorkflowDeadlinePage;
import mx.com.leenustechs.ciaState.models.records.WorkflowPage;
import mx.com.leenustechs.ciaState.models.types.OperationType;
import mx.com.leenustechs.ciaState.models.types.TransactionStatus;
import mx.com.leenustechs.ciaState.support.MemoryEventStateRepository;
import tools.jackson.databind.json.JsonMapper;

class DeadlineScanTest {
    private static final Instant CUTOFF = Instant.ofEpochMilli(10_000);
    private final JsonMapper mapper = JsonMapper.builder().findAndAddModules().build();
    private FirebaseEventStateRepository repository;
    private Query deadlines;
    private Query active;

    @BeforeEach
    void setup() {
        FirebaseDatabase database = mock(FirebaseDatabase.class);
        DatabaseReference root = mock(DatabaseReference.class);
        deadlines = mock(Query.class, RETURNS_SELF);
        active = mock(Query.class, RETURNS_SELF);
        when(database.getReference("workflows")).thenReturn(root);
        when(root.orderByChild("stageDeadlineAt")).thenReturn(deadlines);
        when(root.orderByChild("status")).thenReturn(active);
        repository = new FirebaseEventStateRepository(database, mapper, "workflows", 1);
    }

    private EventStateEntity state(String id, Long deadline, TransactionStatus status) {
        EventStateEntity state = new EventStateEntity();
        state.setTransactionId(id);
        state.setCommand(OperationType.LOGIN);
        state.setStatus(status);
        state.setCurrentStage(0);
        if (deadline != null) state.setStageDeadlineAt(Instant.ofEpochMilli(deadline));
        return state;
    }

    private DataSnapshot row(String key, Object value, Object deadline) {
        DataSnapshot row = mock(DataSnapshot.class);
        DataSnapshot deadlineSnapshot = mock(DataSnapshot.class);
        when(row.getKey()).thenReturn(key);
        when(row.exists()).thenReturn(value != null);
        when(row.getValue()).thenReturn(value);
        when(row.child("stageDeadlineAt")).thenReturn(deadlineSnapshot);
        when(deadlineSnapshot.getValue()).thenReturn(deadline);
        return row;
    }

    private DataSnapshot healthy(String key, long deadline) {
        return row(key, repository.toNativeValue(state(key, deadline, TransactionStatus.PROCESSING)), deadline);
    }

    private void respond(Query query, List<DataSnapshot> rows) {
        DataSnapshot page = mock(DataSnapshot.class);
        when(page.getChildren()).thenReturn(rows);
        doAnswer(call -> {
            call.getArgument(0, ValueEventListener.class).onDataChange(page);
            return null;
        }).when(query).addListenerForSingleValueEvent(any());
    }

    @Test
    void deadlineQueryUsesNumericIndexAndInclusiveCutoff() {
        respond(deadlines, List.of(healthy("a", 1_000), healthy("b", 10_000)));
        var page = repository.scanExpiredWorkflows(CUTOFF, 2, null);
        assertEquals(List.of("a", "b"), page.workflows().stream().map(EventStateEntity::getTransactionId).toList());
        assertEquals(new DeadlineCursor(10_000, "b"), page.nextCursor());
        assertTrue(page.rejectedWorkflowIds().isEmpty());
        verify(deadlines).startAt(0);
        verify(deadlines).endAt(10_000);
        verify(deadlines).limitToFirst(2);
    }

    @Test
    void compoundCursorHandlesEqualDeadlineValuesWithoutRepeatingItsInclusiveRow() {
        DataSnapshot a = healthy("a", 1_000);
        DataSnapshot b = healthy("b", 1_000);
        DataSnapshot c = healthy("c", 2_000);
        respond(deadlines, List.of(a, b, c));
        var page = repository.scanExpiredWorkflows(CUTOFF, 2, new DeadlineCursor(1_000, "a"));
        assertEquals(List.of("b", "c"), page.workflows().stream().map(EventStateEntity::getTransactionId).toList());
        assertEquals(new DeadlineCursor(2_000, "c"), page.nextCursor());
        verify(a, never()).getValue();
        verify(deadlines).startAt(1_000, "a");
        verify(deadlines).limitToFirst(3);
    }

    @Test
    void absentDeadlineCursorStillBoundsRawRowsIncludingRejectedRows() {
        DataSnapshot bad = row("bad", Map.of("stageDeadlineAt", 2_000, "status", "UNKNOWN"), 2_000);
        DataSnapshot beyond = healthy("later", 3_000);
        respond(deadlines, List.of(bad, beyond));
        var page = repository.scanExpiredWorkflows(CUTOFF, 1, new DeadlineCursor(1_000, "missing"));
        assertEquals(new DeadlineCursor(2_000, "bad"), page.nextCursor());
        assertEquals(List.of("bad"), page.rejectedWorkflowIds());
        assertTrue(page.workflows().isEmpty());
        verify(beyond, never()).getValue();
    }

    @Test
    void fractionalDeadlineIsRejectedButItsExactNumericCursorAdvances() {
        Map<String, Object> raw = new LinkedHashMap<>(repository.toNativeValue(state("fraction", 1_000L, TransactionStatus.PROCESSING)));
        raw.put("stageDeadlineAt", 1_000.5);
        DataSnapshot fractional = row("fraction", raw, 1_000.5);
        respond(deadlines, List.of(fractional));
        var first = repository.scanExpiredWorkflows(CUTOFF, 1, null);
        assertEquals(List.of("fraction"), first.rejectedWorkflowIds());
        assertEquals(new DeadlineCursor(1_000.5, "fraction"), first.nextCursor());
        assertTrue(first.workflows().isEmpty());
        respond(deadlines, List.of(fractional, healthy("next", 1_001)));
        var next = repository.scanExpiredWorkflows(CUTOFF, 1, first.nextCursor());
        assertEquals("next", next.workflows().getFirst().getTransactionId());
        verify(deadlines).startAt(1_000.5, "fraction");
    }

    @Test
    void deadlineScanIsolatesMalformedMissingAndMismatchedStates() {
        DataSnapshot malformed = row("a", Map.of("stageDeadlineAt", 1_000, "command", "UNKNOWN"), 1_000);
        DataSnapshot missing = row("b", null, 2_000);
        DataSnapshot mismatch = row("c", repository.toNativeValue(state("other", 3_000L, TransactionStatus.PROCESSING)), 3_000);
        respond(deadlines, List.of(malformed, missing, mismatch));
        var rejected = repository.scanExpiredWorkflows(CUTOFF, 3, null);
        assertTrue(rejected.workflows().isEmpty());
        assertEquals(List.of("a", "b", "c"), rejected.rejectedWorkflowIds());
        assertEquals(new DeadlineCursor(3_000, "c"), rejected.nextCursor());
        respond(deadlines, List.of(mismatch, healthy("d", 4_000)));
        assertEquals("d", repository.scanExpiredWorkflows(CUTOFF, 3, rejected.nextCursor()).workflows().getFirst().getTransactionId());
    }

    @Test
    void deadlineScanReturnsEndAfterEmptyOrCursorOnlyPage() {
        respond(deadlines, List.of());
        assertEquals(new WorkflowDeadlinePage(List.of(), null, List.of()), repository.scanExpiredWorkflows(CUTOFF, 5, null));
        DataSnapshot cursor = healthy("last", 4_000);
        respond(deadlines, List.of(cursor));
        assertNull(repository.scanExpiredWorkflows(CUTOFF, 5, new DeadlineCursor(4_000, "last")).nextCursor());
        verify(cursor, never()).getValue();
    }

    @Test
    void impossibleNumericRangeResultsFailTheScanInsteadOfInventingACursor() {
        for (Object invalid : new Object[]{null, "bad", Double.NaN, Double.POSITIVE_INFINITY, -1, 10_001}) {
            respond(deadlines, List.of(row("bad", Map.of("transactionId", "bad"), invalid)));
            assertThrows(IllegalStateException.class, () -> repository.scanExpiredWorkflows(CUTOFF, 5, null));
        }
    }

    @Test
    void invalidDeadlineBoundsFailOrFinishBeforeIssuingAQuery() {
        assertThrows(IllegalArgumentException.class, () -> repository.scanExpiredWorkflows(CUTOFF, 0, null));
        assertThrows(NullPointerException.class, () -> repository.scanExpiredWorkflows(null, 1, null));
        assertTrue(repository.scanExpiredWorkflows(Instant.ofEpochMilli(-1), 1, null).workflows().isEmpty());
        assertNull(repository.scanExpiredWorkflows(CUTOFF, 1, new DeadlineCursor(10_001, "future")).nextCursor());
        verifyNoInteractions(deadlines);
    }

    @Test
    void activeScanUsesStatusIndexAndKeepsLegacyRowsWithoutDeadlines() {
        DataSnapshot legacy = row("legacy", repository.toNativeValue(state("legacy", null, TransactionStatus.PROCESSING)), null);
        DataSnapshot current = healthy("new", 50_000);
        respond(active, List.of(legacy, current));
        var page = repository.scanProcessingWorkflows(2, null);
        assertEquals(List.of("legacy", "new"), page.workflows().stream().map(EventStateEntity::getTransactionId).toList());
        assertNull(page.workflows().getFirst().getStageDeadlineAt());
        assertEquals("new", page.nextCursor());
        verify(active).equalTo("PROCESSING");
        verify(active).limitToFirst(2);
    }

    @Test
    void activeScanAdvancesAcrossCorruptRowsAndAnAbsentInclusiveCursor() {
        DataSnapshot bad = row("bad", Map.of("transactionId", "bad", "status", "UNKNOWN"), null);
        DataSnapshot healthy = healthy("healthy", 50_000);
        DataSnapshot beyond = healthy("later", 50_000);
        respond(active, List.of(bad, healthy, beyond));
        var first = repository.scanProcessingWorkflows(1, "absent");
        assertEquals("bad", first.nextCursor());
        assertEquals(List.of("bad"), first.rejectedWorkflowIds());
        verify(healthy, never()).getValue();
        respond(active, List.of(bad, healthy, beyond));
        var second = repository.scanProcessingWorkflows(1, first.nextCursor());
        assertEquals("healthy", second.nextCursor());
        assertEquals("healthy", second.workflows().getFirst().getTransactionId());
        verify(active).startAt("PROCESSING", "bad");
        verify(active, times(2)).endAt("PROCESSING");
        verify(active, times(2)).limitToFirst(2);
    }

    @Test
    void activeScanRejectsMismatchedAndTerminalRowsWithoutBlockingHealthyRows() {
        DataSnapshot mismatch = row("a", repository.toNativeValue(state("different", null, TransactionStatus.PROCESSING)), null);
        DataSnapshot terminal = row("b", repository.toNativeValue(state("b", null, TransactionStatus.COMPLETED)), null);
        DataSnapshot disappeared = row("c", null, null);
        respond(active, List.of(mismatch, terminal, disappeared, healthy("d", 20_000)));
        var page = repository.scanProcessingWorkflows(4, null);
        assertEquals(List.of("a", "b", "c"), page.rejectedWorkflowIds());
        assertEquals("d", page.nextCursor());
        assertEquals(1, page.workflows().size());
    }

    @Test
    void activeScanReturnsEndAndRejectsInvalidLimits() {
        assertThrows(IllegalArgumentException.class, () -> repository.scanProcessingWorkflows(-1, null));
        verifyNoInteractions(active);
        respond(active, List.of());
        assertEquals(new WorkflowPage(List.of(), null, List.of()), repository.scanProcessingWorkflows(1, null));
        DataSnapshot cursor = healthy("last", 20_000);
        respond(active, List.of(cursor));
        assertNull(repository.scanProcessingWorkflows(1, "last").nextCursor());
        verify(cursor, never()).getValue();
    }

    @Test
    void scansPropagateCancellationAndPageFailuresWithoutPretendingSuccess() {
        for (Query query : List.of(deadlines, active)) {
            doAnswer(call -> {
                call.getArgument(0, ValueEventListener.class).onCancelled(DatabaseError.fromCode(DatabaseError.PERMISSION_DENIED));
                return null;
            }).when(query).addListenerForSingleValueEvent(any());
        }
        assertThrows(IllegalStateException.class, () -> repository.scanExpiredWorkflows(CUTOFF, 5, null));
        assertThrows(IllegalStateException.class, () -> repository.scanProcessingWorkflows(5, null));
        for (Query query : List.of(deadlines, active)) {
            DataSnapshot broken = mock(DataSnapshot.class);
            when(broken.getChildren()).thenThrow(new IllegalStateException("snapshot unavailable"));
            doAnswer(call -> {
                call.getArgument(0, ValueEventListener.class).onDataChange(broken);
                return null;
            }).when(query).addListenerForSingleValueEvent(any());
        }
        assertThrows(IllegalStateException.class, () -> repository.scanExpiredWorkflows(CUTOFF, 5, null));
        assertThrows(IllegalStateException.class, () -> repository.scanProcessingWorkflows(5, null));
    }

    @Test
    void pagesCopyListsAndNumericCursorRejectsUnusableValues() {
        var rows = new ArrayList<EventStateEntity>();
        var rejected = new ArrayList<>(List.of("bad"));
        var deadlinePage = new WorkflowDeadlinePage(rows, new DeadlineCursor(1, "bad"), rejected);
        var activePage = new WorkflowPage(rows, "bad", rejected);
        rows.add(new EventStateEntity()); rejected.clear();
        assertTrue(deadlinePage.workflows().isEmpty());
        assertEquals(List.of("bad"), deadlinePage.rejectedWorkflowIds());
        assertEquals(List.of("bad"), activePage.rejectedWorkflowIds());
        assertThrows(UnsupportedOperationException.class, () -> activePage.workflows().clear());
        assertThrows(UnsupportedOperationException.class, () -> deadlinePage.rejectedWorkflowIds().clear());
        for (double number : new double[]{-1, Double.NaN, Double.POSITIVE_INFINITY}) {
            assertThrows(IllegalArgumentException.class, () -> new DeadlineCursor(number, "key"));
        }
        assertThrows(IllegalArgumentException.class, () -> new DeadlineCursor(1, null));
        assertThrows(IllegalArgumentException.class, () -> new DeadlineCursor(1, ""));
    }

    @Test
    void memoryRepositoryModelsDeadlineOrderingCutoffAndLegacyScan() {
        var memory = new MemoryEventStateRepository();
        for (EventStateEntity row : List.of(state("b", 1_000L, TransactionStatus.PROCESSING),
                state("a", 1_000L, TransactionStatus.PROCESSING), state("c", 2_000L, TransactionStatus.COMPLETED),
                state("legacy", null, TransactionStatus.PROCESSING))) {
            memory.updateAtomically(row.getTransactionId(), ignored -> java.util.Optional.of(row));
        }
        var first = memory.scanExpiredWorkflows(Instant.ofEpochMilli(1_000), 1, null);
        assertEquals("a", first.workflows().getFirst().getTransactionId());
        var second = memory.scanExpiredWorkflows(Instant.ofEpochMilli(1_000), 1, first.nextCursor());
        assertEquals("b", second.workflows().getFirst().getTransactionId());
        assertNull(memory.scanExpiredWorkflows(Instant.ofEpochMilli(1_000), 1, second.nextCursor()).nextCursor());
        assertEquals(List.of("a", "b", "legacy"), memory.scanProcessingWorkflows(10, null).workflows().stream()
                .map(EventStateEntity::getTransactionId).toList());
        assertEquals(List.of("b", "legacy"), memory.scanProcessingWorkflows(10, "a").workflows().stream()
                .map(EventStateEntity::getTransactionId).toList());
        assertNull(memory.scanProcessingWorkflows(10, "legacy").nextCursor());
    }
}
