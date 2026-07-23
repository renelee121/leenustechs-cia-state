package mx.com.leenustechs.ciaState.business.utils.mappers;

import javax.annotation.processing.Generated;
import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.requests.CommonModelRequest;
import mx.com.leenustechs.ciaState.models.responses.CommonModelResponse;
import mx.com.leenustechs.ciaState.models.types.OperationType;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

@Generated(
    value = "org.mapstruct.ap.MappingProcessor",
    date = "2026-07-22T19:40:20-0600",
    comments = "version: 1.6.3, compiler: javac, environment: Java 21.0.11 (Arch Linux)"
)
@Component
public class CommonModelMapperImpl implements CommonModelMapper {

    @Override
    public CommonModel toModel(CommonModelRequest request, String transactionId, String producer, OperationType command) {
        if ( request == null && transactionId == null && producer == null && command == null ) {
            return null;
        }

        JsonNode payload = null;
        if ( request != null ) {
            payload = request.getPayload();
        }
        String transactionId1 = null;
        transactionId1 = transactionId;
        String producer1 = null;
        producer1 = producer;
        OperationType command1 = null;
        command1 = command;

        CommonModel commonModel = new CommonModel( transactionId1, producer1, command1, payload );

        return commonModel;
    }

    @Override
    public CommonModelResponse toResponse(CommonModel model) {
        if ( model == null ) {
            return null;
        }

        String transactionId = null;
        String producer = null;
        OperationType command = null;
        JsonNode payload = null;

        transactionId = model.getTransactionId();
        producer = model.getProducer();
        command = model.getCommand();
        payload = model.getPayload();

        CommonModelResponse commonModelResponse = new CommonModelResponse( transactionId, producer, command, payload );

        return commonModelResponse;
    }
}
