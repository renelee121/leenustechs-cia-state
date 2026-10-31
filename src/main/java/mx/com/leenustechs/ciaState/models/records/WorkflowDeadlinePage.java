package mx.com.leenustechs.ciaState.models.records;

import java.util.List;
import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;

/** Cursor advances over raw deadline rows, including rejected workflows. */
public record WorkflowDeadlinePage(List<EventStateEntity> workflows, DeadlineCursor nextCursor,
        List<String> rejectedWorkflowIds) {
    public WorkflowDeadlinePage {
        workflows = List.copyOf(workflows);
        rejectedWorkflowIds = List.copyOf(rejectedWorkflowIds);
    }
}
