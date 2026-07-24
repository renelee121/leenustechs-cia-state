package mx.com.leenustechs.ciaState.business.utils.mappers;

import java.util.EnumMap;

import org.mapstruct.AfterMapping;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import org.mapstruct.MappingTarget;
import org.mapstruct.ReportingPolicy;

import mx.com.leenustechs.ciaState.models.EventStateModel;
import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;
import mx.com.leenustechs.ciaState.models.responses.EventStateResponse;
import mx.com.leenustechs.ciaState.models.types.StepType;
import tools.jackson.databind.node.ObjectNode;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING, unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface EventStateModelMapper {

    EventStateModel toModel(EventStateEntity entity);

    EventStateResponse toResponse(EventStateModel model);

    @Mapping(target = "ttl", ignore = true)
    EventStateEntity toEntity(EventStateModel model);

    @Mapping(target = "payload", ignore = true)
    @Mapping(target = "steps", ignore = true)
    @Mapping(target = "ttl", ignore = true)
    @Mapping(target = "createdAt", ignore = true)
    void updateFromModel(
            EventStateModel source,
            @MappingTarget EventStateEntity target);

    default void mergePayload(
            EventStateModel source,
            @MappingTarget EventStateEntity target) {

        if (source.getPayload() == null) {
            return;
        }

        if (target.getPayload() == null) {
            target.setPayload(source.getPayload().deepCopy());
            return;
        }

        if (target.getPayload().isObject()
                && source.getPayload().isObject()) {

            ((ObjectNode) target.getPayload())
                    .setAll((ObjectNode) source.getPayload());
        }
    }

    default void mergeSteps(
            EventStateModel source,
            @MappingTarget EventStateEntity target) {

        if (source.getSteps() == null) {
            return;
        }

        if (target.getSteps() == null) {
            target.setSteps(new EnumMap<>(StepType.class));
        }

        target.getSteps().putAll(source.getSteps());
    }

    @AfterMapping
    default void afterMapping(
            EventStateModel source,
            @MappingTarget EventStateEntity target) {

        mergePayload(source, target);
        mergeSteps(source, target);
    }
}