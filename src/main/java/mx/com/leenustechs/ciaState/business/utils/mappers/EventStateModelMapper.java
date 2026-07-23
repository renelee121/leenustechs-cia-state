package mx.com.leenustechs.ciaState.business.utils.mappers;

import org.mapstruct.AfterMapping;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import org.mapstruct.MappingTarget;
import org.mapstruct.ReportingPolicy;

import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.EventStateModel;
import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;
import mx.com.leenustechs.ciaState.models.responses.EventStateResponse;
import tools.jackson.databind.node.ObjectNode;

@Mapper(
    componentModel = MappingConstants.ComponentModel.SPRING,
    unmappedTargetPolicy = ReportingPolicy.ERROR
)
public interface EventStateModelMapper {

    EventStateModel toModel(EventStateEntity entity);

    EventStateResponse toResponse(EventStateModel model);

    @Mapping(target = "payload", ignore = true)
    void updateFromCommonModel(
        CommonModel source,
        @MappingTarget EventStateModel target
    );

    default void mergePayload(
            CommonModel source,
            @MappingTarget EventStateModel target) {

        if (source.getPayload() == null) {
            return;
        }

        if (target.getPayload() == null) {
            target.setPayload(source.getPayload().deepCopy());
            return;
        }

        if (target.getPayload().isObject() && source.getPayload().isObject()) {
            ((ObjectNode) target.getPayload())
                    .setAll((ObjectNode) source.getPayload());
        }
    }

    @AfterMapping
    default void afterMapping(
            CommonModel source,
            @MappingTarget EventStateModel target) {

        mergePayload(source, target);
    }
}