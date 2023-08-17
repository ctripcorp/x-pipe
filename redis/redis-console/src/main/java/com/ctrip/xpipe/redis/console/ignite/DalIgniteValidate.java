package com.ctrip.xpipe.redis.console.ignite;

import com.ctrip.platform.dal.dao.DalClientFactory;
import com.ctrip.platform.dal.dao.configure.ignite.IgniteResult;
import com.ctrip.platform.dal.exceptions.DalRuntimeException;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;


@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class DalIgniteValidate{

    @PostConstruct
    public void init() {
        IgniteResult igniteResult = DalClientFactory.checkConnections();
        if (igniteResult.anyFail()) {
            throw new DalRuntimeException(igniteResult.getErrorMsg());
        }
    }
}
