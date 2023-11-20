package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;

public abstract class AbstractKeeperInfoContext extends AbstractActionContext<InfoResultExtractor, KeeperHealthCheckInstance> {
    public AbstractKeeperInfoContext(KeeperHealthCheckInstance instance, InfoResultExtractor infoResultExtractor) {
        super(instance, infoResultExtractor);
    }
}
