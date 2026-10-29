package mx.com.leenustechs.ciaState.business.services;

import java.util.List;
import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.records.WorkflowStage;
import mx.com.leenustechs.ciaState.models.responses.EventStateResponse;

public interface EventStateService {
    EventStateResponse process(CommonModel event, List<WorkflowStage> workflow);
    EventStateResponse findByTransactionId(String transactionId);
    /** Expires a due stage, or grants a grace deadline to a legacy active workflow. */
    boolean reconcile(String transactionId);
}
