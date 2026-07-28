package mx.com.leenustechs.ciaState.business.services.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import mx.com.leenustechs.ciaState.business.utils.commons.EventOperation;
import mx.com.leenustechs.ciaState.business.utils.exceptions.EmptyOperationResponseException;
import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.responses.CommonModelResponse;
import mx.com.leenustechs.ciaState.models.types.OperationType;

class CommandDispatcherServiceImplTest {

    @Test
    void dispatchesTheEventToItsRegisteredOperation() {
        CommonModel event = event(OperationType.LOGIN);
        CommonModelResponse expected = new CommonModelResponse("tx", "API", OperationType.LOGIN, null);
        EventOperation operation = operationFor(OperationType.LOGIN);
        when(operation.execute(event)).thenReturn(expected);

        CommonModelResponse actual =
                new CommandDispatcherServiceImpl(List.of(operation)).execute(event);

        assertEquals(expected, actual);
    }

    @Test
    void rejectsDuplicateOperationRegistrations() {
        EventOperation first = operationFor(OperationType.LOGIN);
        EventOperation second = operationFor(OperationType.LOGIN);

        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> new CommandDispatcherServiceImpl(List.of(first, second)));

        assertEquals("Duplicate operation type: LOGIN", exception.getMessage());
    }

    @Test
    void rejectsUnknownOperations() {
        CommandDispatcherServiceImpl dispatcher =
                new CommandDispatcherServiceImpl(List.of(operationFor(OperationType.LOGIN)));

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> dispatcher.execute(event(OperationType.LOGOUT)));

        assertEquals("No operation found for command: LOGOUT", exception.getMessage());
    }

    @Test
    void rejectsNullOperationResponsesWithTransactionContext() {
        CommonModel event = event(OperationType.LOGIN);
        EventOperation operation = operationFor(OperationType.LOGIN);
        when(operation.execute(event)).thenReturn(null);

        EmptyOperationResponseException exception = assertThrows(
                EmptyOperationResponseException.class,
                () -> new CommandDispatcherServiceImpl(List.of(operation)).execute(event));

        assertEquals("tx", exception.getTransactionId());
        assertEquals(OperationType.LOGIN, exception.getOperationType());
        assertEquals(
                "Transaction 'tx' with operation 'LOGIN' returned no response",
                exception.getMessage());
    }

    private EventOperation operationFor(OperationType type) {
        EventOperation operation = mock(EventOperation.class);
        when(operation.getEventTypes()).thenReturn(Set.of(type));
        return operation;
    }

    private CommonModel event(OperationType type) {
        return new CommonModel("tx", "API", type, null);
    }
}
