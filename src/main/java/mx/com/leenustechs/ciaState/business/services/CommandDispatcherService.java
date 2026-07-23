package mx.com.leenustechs.ciaState.business.services;

import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.responses.CommonModelResponse;

public interface CommandDispatcherService {
    public CommonModelResponse execute(CommonModel event);
}
