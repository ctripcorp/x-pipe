package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.info;

import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractInfoContext;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;

public class RedisInfoActionContext extends AbstractInfoContext<RedisHealthCheckInstance> {

    public RedisInfoActionContext(RedisHealthCheckInstance instance, String extractor) {
        super(instance, new InfoResultExtractor(extractor));
    }
}
