package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;

public abstract class AbstractInfoContext extends AbstractActionContext<InfoResultExtractor, RedisHealthCheckInstance> {
    public AbstractInfoContext(RedisHealthCheckInstance instance, InfoResultExtractor extractor) {
        super(instance, extractor);
    }
}
