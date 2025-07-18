package com.ctrip.xpipe.service.ignite;

import com.ctrip.platform.dal.dao.DalClientFactory;
import com.ctrip.platform.dal.dao.configure.ignite.IgniteResult;
import com.ctrip.platform.dal.dao.configure.ignite.SingleIgniteResult;
import com.ctrip.platform.dal.exceptions.DalRuntimeException;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class DalIgniteValidate {

    private static final Logger log = LoggerFactory.getLogger(DalIgniteValidate.class);

    @PostConstruct
    public void init() {
        IgniteResult igniteResult = DalClientFactory.checkConnections();
        if (igniteResult.anyFail()) {
            for(SingleIgniteResult result : igniteResult.getIgniteDetail()) {
                log.error("Ignite check fail", result.getError());
            }
            throw new DalRuntimeException(igniteResult.getErrorMsg());
        }
    }
}
