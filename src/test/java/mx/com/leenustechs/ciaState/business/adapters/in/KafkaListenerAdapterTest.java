package mx.com.leenustechs.ciaState.business.adapters.in;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;

import mx.com.leenustechs.ciaState.business.services.CommandDispatcherService;
import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.types.EventOutcome;
import mx.com.leenustechs.ciaState.models.types.OperationType;

class KafkaListenerAdapterTest {
    private final CommandDispatcherService dispatcher = mock(CommandDispatcherService.class);
    private final KafkaListenerAdapter listener = new KafkaListenerAdapter(dispatcher);

    private CommonModel event(String producer) {
        return new CommonModel("tx-1", producer, OperationType.LOGIN, null);
    }

    @Test
    void routesInitialAndWorkerEvents() {
        CommonModel initial = event("CIA_API");
        CommonModel worker = event("SECURITY");
        listener.lobbyListener(initial);
        listener.responseListener(worker);
        verify(dispatcher).execute(initial);
        verify(dispatcher).execute(worker);
    }

    @Test
    void failureTopicsSetExplicitOutcomeWithoutInspectingPayload() {
        CommonModel failure = event("SECURITY");
        CommonModel deadLetter = event("ASSETS");
        listener.failureResponseListener(failure);
        listener.deadLetterListener(deadLetter);
        assertEquals(EventOutcome.FAILURE, failure.getOutcome());
        assertEquals(EventOutcome.FAILURE, deadLetter.getOutcome());
        verify(dispatcher).execute(failure);
        verify(dispatcher).execute(deadLetter);
    }

    @Test
    void preventsWorkerEventsOnLobbyAndInitialEventsOnResponses() {
        assertThrows(IllegalArgumentException.class, () -> listener.lobbyListener(event("SECURITY")));
        assertThrows(IllegalArgumentException.class, () -> listener.responseListener(event("CIA_API")));
        assertThrows(IllegalArgumentException.class, () -> listener.failureResponseListener(event("CIA_API")));
        verifyNoInteractions(dispatcher);
    }

    @Test
    void rejectsTombstonesAndInvalidEnvelopes() {
        assertThrows(IllegalArgumentException.class, () -> listener.responseListener(null));
        assertThrows(IllegalArgumentException.class, () -> listener.lobbyListener(new CommonModel()));
        verifyNoInteractions(dispatcher);
    }
}
