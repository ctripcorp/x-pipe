package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstance;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;

public abstract class AbstractInfoContext<T extends HealthCheckInstance> extends AbstractActionContext<InfoResultExtractor, T> {
    public AbstractInfoContext(T instance, InfoResultExtractor extractor) {
        super(instance, extractor);
    }
}
