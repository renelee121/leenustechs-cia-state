package mx.com.leenustechs.ciaState.models.records;

import java.util.List;
import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;

/** A bounded scan result whose cursor advances over healthy and rejected rows. */
public record OutboxPage(
        List<EventStateEntity> workflows,
        String nextCursor,
        List<String> rejectedWorkflowIds) {
    public OutboxPage {
        workflows = List.copyOf(workflows);
        rejectedWorkflowIds = List.copyOf(rejectedWorkflowIds);
    }
}
