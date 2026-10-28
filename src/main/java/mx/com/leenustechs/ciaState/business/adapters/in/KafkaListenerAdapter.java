package mx.com.leenustechs.ciaState.business.adapters.in;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import mx.com.leenustechs.ciaState.business.services.CommandDispatcherService;
import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.constants.KafkaTopics;
import mx.com.leenustechs.ciaState.models.types.EventOutcome;

@Component
@RequiredArgsConstructor
public class KafkaListenerAdapter {
    private static final String INITIAL_PRODUCER = "CIA_API";
    private final CommandDispatcherService operationTypeService;

    @KafkaListener(topics = KafkaTopics.LEENUSTECHS_CIA_LOBBY, containerFactory = "kafkaListenerContainerFactory")
    public void lobbyListener(CommonModel event) {
        validate(event, true);
        operationTypeService.execute(event);
    }

    @KafkaListener(topics = KafkaTopics.LEENUSTECHS_CIA_RESPONSE, containerFactory = "kafkaListenerContainerFactory")
    public void responseListener(CommonModel event) {
        validate(event, false);
        operationTypeService.execute(event);
    }

    @KafkaListener(topics = KafkaTopics.LEENUSTECHS_CIA_DEADLETTER, containerFactory = "kafkaListenerContainerFactory")
    public void deadLetterListener(CommonModel event) {
        dispatchFailure(event);
    }

    @KafkaListener(topics = KafkaTopics.LEENUSTECHS_CIA_FAILURE_RESPONSE, containerFactory = "kafkaListenerContainerFactory")
    public void failureResponseListener(CommonModel event) {
        dispatchFailure(event);
    }

    private void dispatchFailure(CommonModel event) {
        validate(event, false);
        event.setOutcome(EventOutcome.FAILURE);
        operationTypeService.execute(event);
    }

    private void validate(CommonModel event, boolean initial) {
        if (event == null) {
            throw new IllegalArgumentException("A workflow event is required; tombstones are not supported");
        }
        event.validate();
        if (initial != INITIAL_PRODUCER.equals(event.getProducer())) {
            throw new IllegalArgumentException("Event producer is not valid for this topic");
        }
    }
}
