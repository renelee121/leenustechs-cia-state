package mx.com.leenustechs.ciaState.business.utils.commons;

import java.util.Set;

import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.responses.CommonModelResponse;
import mx.com.leenustechs.ciaState.models.types.OperationType;

public interface EventOperation {
    public CommonModelResponse execute(CommonModel event);
    public Set<OperationType> getEventTypes();
}
