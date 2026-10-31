package mx.com.leenustechs.ciaState.business.services.impl;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
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
    void dispatchesValidatedEventToItsRegisteredOperation() {
        EventOperation login = operation(OperationType.LOGIN);
        EventOperation logout = operation(OperationType.LOGOUT);
        CommonModel event = valid();
        CommonModelResponse expected = new CommonModelResponse();
        when(login.execute(event)).thenReturn(expected);
        assertThat(new CommandDispatcherServiceImpl(List.of(login, logout)).execute(event)).isSameAs(expected);
        verify(login).execute(event);
        verify(logout, never()).execute(any());
    }

    @Test
    void rejectsDuplicateCommandRegistrationAtStartup() {
        assertThatThrownBy(() -> new CommandDispatcherServiceImpl(List.of(operation(OperationType.LOGIN), operation(OperationType.LOGIN))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Duplicate operation type: LOGIN");
    }

    @Test
    void rejectsUnknownCommandsAndEmptyOperationResponses() {
        CommandDispatcherServiceImpl empty = new CommandDispatcherServiceImpl(List.of());
        assertThatThrownBy(() -> empty.execute(valid())).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No operation found");
        EventOperation operation = operation(OperationType.LOGIN);
        assertThatThrownBy(() -> new CommandDispatcherServiceImpl(List.of(operation)).execute(valid()))
                .isInstanceOfSatisfying(EmptyOperationResponseException.class, error -> {
                    assertThat(error.getTransactionId()).isEqualTo("tx-1");
                    assertThat(error.getOperationType()).isEqualTo(OperationType.LOGIN);
                    assertThat(error.getMessage()).contains("tx-1", "LOGIN");
                });
    }

    @Test
    void invalidInputNeverReachesTheOperation() {
        EventOperation operation = operation(OperationType.LOGIN);
        CommandDispatcherServiceImpl dispatcher = new CommandDispatcherServiceImpl(List.of(operation));
        assertThatThrownBy(() -> dispatcher.execute(null)).isInstanceOf(IllegalArgumentException.class);
        CommonModel invalid = valid();
        invalid.setTransactionId("path/injection");
        assertThatThrownBy(() -> dispatcher.execute(invalid)).isInstanceOf(IllegalArgumentException.class);
        verify(operation, never()).execute(any());
    }

    private EventOperation operation(OperationType type) {
        EventOperation operation = mock(EventOperation.class);
        when(operation.getEventTypes()).thenReturn(Set.of(type));
        return operation;
    }

    private CommonModel valid() {
        return new CommonModel("tx-1", "CIA_API", OperationType.LOGIN, null);
    }
}
