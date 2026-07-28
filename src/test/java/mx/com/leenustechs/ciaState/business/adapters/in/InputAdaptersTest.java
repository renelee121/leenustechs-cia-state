package mx.com.leenustechs.ciaState.business.adapters.in;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import mx.com.leenustechs.ciaState.business.services.CommandDispatcherService;
import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.types.OperationType;

class InputAdaptersTest {

    @Test
    void interceptorAddsTracingDataAndCleansTimingData() throws Exception {
        ApiInterceptor interceptor = new ApiInterceptor();
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        java.util.Map<String, Object> attributes = new java.util.HashMap<>();
        org.mockito.Mockito.doAnswer(invocation -> {
            attributes.put(invocation.getArgument(0), invocation.getArgument(1));
            return null;
        }).when(request).setAttribute(org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.any());
        when(request.getAttribute(org.mockito.ArgumentMatchers.anyString()))
                .thenAnswer(invocation -> attributes.get(invocation.getArgument(0)));
        org.mockito.Mockito.doAnswer(invocation -> {
            attributes.remove(invocation.getArgument(0));
            return null;
        }).when(request).removeAttribute(org.mockito.ArgumentMatchers.anyString());

        assertTrue(interceptor.preHandle(request, response, new Object()));
        assertNotNull(attributes.get("transactionId"));
        assertNotNull(attributes.get("startTime"));

        interceptor.afterCompletion(request, response, new Object(), null);

        assertNull(attributes.get("startTime"));
    }

    @Test
    void everyKafkaListenerDelegatesToTheDispatcher() {
        CommandDispatcherService dispatcher = mock(CommandDispatcherService.class);
        KafkaListenerAdapter adapter = new KafkaListenerAdapter(dispatcher);
        CommonModel event = new CommonModel("tx", "API", OperationType.LOGIN, null);

        adapter.lobbyListener(event);
        adapter.responseListener(event);
        adapter.deadLetterListener(event);
        adapter.failureResponseListener(event);

        verify(dispatcher, times(4)).execute(event);
    }
}
