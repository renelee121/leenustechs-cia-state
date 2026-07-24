package mx.com.leenustechs.ciaState.business.utils.mappers;

import java.time.Instant;
import java.util.Map;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import org.mapstruct.ReportingPolicy;

import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.EventStateModel;
import mx.com.leenustechs.ciaState.models.types.StepStatus;
import mx.com.leenustechs.ciaState.models.types.StepType;
import mx.com.leenustechs.ciaState.models.types.TransactionStatus;
import tools.jackson.databind.JsonNode;

@Mapper(
    componentModel = MappingConstants.ComponentModel.SPRING,
    unmappedTargetPolicy = ReportingPolicy.ERROR
)
public interface EventStateInputMapper {

    @Mapping(target = "transactionId", source = "commonModel.transactionId")
    @Mapping(target = "producer", source = "commonModel.producer")
    @Mapping(target = "command", source = "commonModel.command")
    @Mapping(target = "payload", source = "commonModel.payload")

    @Mapping(target = "status", source = "status")
    @Mapping(target = "currentStage", source = "currentStage")
    @Mapping(target = "steps", source = "steps")
    @Mapping(target = "result", source = "result")
    @Mapping(target = "createdAt", source = "createdAt")
    @Mapping(target = "updatedAt", source = "updatedAt")
    EventStateModel toEventStateModel(
            CommonModel commonModel,
            TransactionStatus status,
            Integer currentStage,
            Map<StepType, StepStatus> steps,
            JsonNode result,
            Instant createdAt,
            Instant updatedAt
    );
}