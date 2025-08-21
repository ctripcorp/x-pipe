package com.ctrip.xpipe.service.ignite;

import com.ctrip.platform.dal.dao.DalClientFactory;
import com.ctrip.platform.dal.dao.configure.ignite.IgniteResult;
import com.ctrip.platform.dal.exceptions.DalRuntimeException;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

@Component
public class DalIgniteValidate {

    @PostConstruct
    public void init() {
        IgniteResult igniteResult = DalClientFactory.checkConnections();
        if (igniteResult.anyFail()) {
            throw new DalRuntimeException(igniteResult.getErrorMsg());
        }
    }
}
