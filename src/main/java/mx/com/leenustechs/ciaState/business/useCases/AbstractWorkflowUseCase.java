package mx.com.leenustechs.ciaState.business.useCases;

import java.util.List;

import mx.com.leenustechs.ciaState.business.services.EventStateService;
import mx.com.leenustechs.ciaState.business.utils.commons.EventOperation;
import mx.com.leenustechs.ciaState.business.utils.mappers.CommonModelMapper;
import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.records.WorkflowStage;
import mx.com.leenustechs.ciaState.models.responses.CommonModelResponse;
import mx.com.leenustechs.ciaState.models.responses.EventStateResponse;

/** Declares a workflow; the state service persists transitions and outgoing events atomically. */
public abstract class AbstractWorkflowUseCase implements EventOperation {
    private final CommonModelMapper commonModelMapper;
    private final EventStateService eventStateService;

    protected AbstractWorkflowUseCase(CommonModelMapper commonModelMapper, EventStateService eventStateService) {
        this.commonModelMapper = commonModelMapper;
        this.eventStateService = eventStateService;
    }

    protected abstract List<WorkflowStage> workflow();

    @Override
    public final CommonModelResponse execute(CommonModel event) {
        EventStateResponse state = eventStateService.process(event, workflow());
        CommonModelResponse response = commonModelMapper.toResponse(event);
        response.setStatus(state.getStatus());
        return response;
    }
}
