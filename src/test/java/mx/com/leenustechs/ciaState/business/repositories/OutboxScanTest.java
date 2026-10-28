package mx.com.leenustechs.ciaState.business.repositories;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.Query;
import com.google.firebase.database.ValueEventListener;
import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;
import mx.com.leenustechs.ciaState.models.records.OutboxEvent;
import mx.com.leenustechs.ciaState.models.records.OutboxPage;
import mx.com.leenustechs.ciaState.models.types.OperationType;
import mx.com.leenustechs.ciaState.models.types.TransactionStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class OutboxScanTest {
    private final JsonMapper mapper = JsonMapper.builder().findAndAddModules().build();
    private FirebaseEventStateRepository repository;
    private Query query;

    @BeforeEach
    void setup() {
        FirebaseDatabase database = mock(FirebaseDatabase.class);
        DatabaseReference root = mock(DatabaseReference.class);
        query = mock(Query.class, RETURNS_SELF);
        when(database.getReference("workflows")).thenReturn(root);
        when(root.orderByChild("outboxPending")).thenReturn(query);
        repository = new FirebaseEventStateRepository(database, mapper, "workflows", 1);
    }

    private DataSnapshot row(String key, Object value) {
        DataSnapshot row = mock(DataSnapshot.class);
        when(row.getKey()).thenReturn(key);
        when(row.exists()).thenReturn(value != null);
        when(row.getValue()).thenReturn(value);
        return row;
    }

    private DataSnapshot healthy(String key) {
        EventStateEntity state = new EventStateEntity();
        state.setTransactionId(key);
        state.setCommand(OperationType.LOGIN);
        state.setStatus(TransactionStatus.PROCESSING);
        state.setCurrentStage(0);
        CommonModel event = new CommonModel(key, "CIA_STATE", OperationType.LOGIN, mapper.createObjectNode());
        event.setEventId("event-" + key);
        state.setPendingOutbox(Map.of(event.getEventId(), new OutboxEvent(event.getEventId(), "topic", key, event)));
        return row(key, repository.toNativeValue(state));
    }

    private void respondWith(List<DataSnapshot> rows) {
        DataSnapshot snapshot = mock(DataSnapshot.class);
        when(snapshot.getChildren()).thenReturn(rows);
        doAnswer(call -> {
            call.getArgument(0, ValueEventListener.class).onDataChange(snapshot);
            return null;
        }).when(query).addListenerForSingleValueEvent(any());
    }

    @Test
    void skipsCorruptRowWithoutDiscardingHealthyNeighborsAndBoundsRawRows() {
        DataSnapshot corrupt = row("a", "invalid state with private data");
        DataSnapshot healthy = healthy("b");
        DataSnapshot beyondLimit = healthy("c");
        respondWith(List.of(corrupt, healthy, beyondLimit));

        OutboxPage page = repository.scanPendingOutbox(2, null);

        assertEquals(List.of("b"), page.workflows().stream().map(EventStateEntity::getTransactionId).toList());
        assertEquals("b", page.nextCursor());
        assertEquals(List.of("a"), page.rejectedWorkflowIds());
        verify(query).equalTo(true);
        verify(query).limitToFirst(2);
        verify(beyondLimit, never()).getValue();
    }

    @Test
    void allCorruptPageStillAdvancesCursorAndNextPageCanDeliverHealthyWork() {
        DataSnapshot a = row("a", Map.of("pendingOutbox", "malformed"));
        DataSnapshot b = row("b", List.of("invalid"));
        respondWith(List.of(a, b));
        OutboxPage poisoned = repository.scanPendingOutbox(2, null);
        assertTrue(poisoned.workflows().isEmpty());
        assertEquals("b", poisoned.nextCursor());
        assertEquals(List.of("a", "b"), poisoned.rejectedWorkflowIds());

        DataSnapshot c = healthy("c");
        respondWith(List.of(b, c));
        OutboxPage next = repository.scanPendingOutbox(2, poisoned.nextCursor());
        assertEquals(List.of("c"), next.workflows().stream().map(EventStateEntity::getTransactionId).toList());
        assertEquals("c", next.nextCursor());
        assertTrue(next.rejectedWorkflowIds().isEmpty());
        verify(query).startAt(true, "b");
        verify(query).endAt(true);
        verify(query).limitToFirst(3);
    }

    @Test
    void absentInclusiveCursorStillExaminesNoMoreThanLimitRows() {
        DataSnapshot b = healthy("b");
        DataSnapshot c = row("c", 42);
        DataSnapshot d = healthy("d");
        respondWith(List.of(b, c, d));

        OutboxPage page = repository.scanPendingOutbox(2, "a");

        assertEquals("c", page.nextCursor());
        assertEquals(List.of("c"), page.rejectedWorkflowIds());
        assertEquals(1, page.workflows().size());
        verify(d, never()).getValue();
    }

    @Test
    void emptyPageAndCursorOnlyPageSignalEndOfScan() {
        respondWith(List.of());
        assertEquals(new OutboxPage(List.of(), null, List.of()), repository.scanPendingOutbox(5, null));
        DataSnapshot cursor = row("last", "already examined");
        respondWith(List.of(cursor));
        assertNull(repository.scanPendingOutbox(5, "last").nextCursor());
        verify(cursor, never()).getValue();
    }

    @Test
    void mismatchedOrDisappearedWorkflowIsIsolatedByDatabaseKey() {
        DataSnapshot mismatch = row("a", Map.of("transactionId", "different"));
        DataSnapshot disappeared = row("b", null);
        respondWith(List.of(mismatch, disappeared));

        OutboxPage page = repository.scanPendingOutbox(2, null);

        assertTrue(page.workflows().isEmpty());
        assertEquals("b", page.nextCursor());
        assertEquals(List.of("a", "b"), page.rejectedWorkflowIds());
    }

    @Test
    void existingFindMethodsReturnOnlyHealthyRows() {
        DataSnapshot healthy = healthy("good");
        DataSnapshot invalid = row("invalid", false);
        respondWith(List.of(healthy, invalid));
        assertEquals(1, repository.findWithPendingOutbox(2).size());
        assertEquals(1, repository.findWithPendingOutbox(2, "earlier").size());
    }

    @Test
    void infrastructureFailuresAreReportedInsteadOfPretendingThePageIsEmpty() {
        doAnswer(call -> {
            call.getArgument(0, ValueEventListener.class).onCancelled(
                    DatabaseError.fromCode(DatabaseError.PERMISSION_DENIED));
            return null;
        }).when(query).addListenerForSingleValueEvent(any());
        assertThrows(IllegalStateException.class, () -> repository.scanPendingOutbox(5, null));
        DataSnapshot brokenPage = mock(DataSnapshot.class);
        when(brokenPage.getChildren()).thenThrow(new IllegalStateException("snapshot unavailable"));
        doAnswer(call -> {
            call.getArgument(0, ValueEventListener.class).onDataChange(brokenPage);
            return null;
        }).when(query).addListenerForSingleValueEvent(any());
        assertThrows(IllegalStateException.class, () -> repository.scanPendingOutbox(5, null));
    }

    @Test
    void invalidLimitsFailBeforeIssuingAQuery() {
        assertThrows(IllegalArgumentException.class, () -> repository.scanPendingOutbox(0, null));
        assertThrows(IllegalArgumentException.class, () -> repository.scanPendingOutbox(-1, "cursor"));
        verifyNoInteractions(query);
    }

    @Test
    void scanPageCopiesItsListsAndRejectsMutation() {
        List<EventStateEntity> states = new ArrayList<>();
        List<String> rejected = new ArrayList<>(List.of("corrupt"));
        OutboxPage page = new OutboxPage(states, "corrupt", rejected);
        states.add(new EventStateEntity());
        rejected.clear();
        assertTrue(page.workflows().isEmpty());
        assertEquals(List.of("corrupt"), page.rejectedWorkflowIds());
        assertThrows(UnsupportedOperationException.class, () -> page.workflows().clear());
        assertThrows(UnsupportedOperationException.class, () -> page.rejectedWorkflowIds().clear());
    }
}
