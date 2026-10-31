package mx.com.leenustechs.ciaState.models.records;

import java.util.List;
import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;

/** Bounded active-workflow page for legacy deadline initialization. */
public record WorkflowPage(List<EventStateEntity> workflows, String nextCursor,
        List<String> rejectedWorkflowIds) {
    public WorkflowPage {
        workflows = List.copyOf(workflows);
        rejectedWorkflowIds = List.copyOf(rejectedWorkflowIds);
    }
}
