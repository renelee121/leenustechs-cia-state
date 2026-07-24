package mx.com.leenustechs.ciaState.business.adapters.in;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mx.com.leenustechs.ciaState.business.services.CommandDispatcherService;
import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.constants.KafkaTopics;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaListenerAdapter {
    private final CommandDispatcherService operationTypeService;

    @KafkaListener(topics = KafkaTopics.LEENUSTECHS_CIA_LOBBY, containerFactory = "kafkaListenerContainerFactory")
    public void lobbyListener(CommonModel event){
        operationTypeService.execute(event);
    }
    
    @KafkaListener(topics = KafkaTopics.LEENUSTECHS_CIA_RESPONSE, containerFactory = "kafkaListenerContainerFactory")
    public void responseListener(CommonModel event){
        operationTypeService.execute(event);
    }
}
