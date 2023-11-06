package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.inforeplication;

import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractInfoContext;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;

public class InfoReplicationContext extends AbstractInfoContext<RedisHealthCheckInstance> {

    public InfoReplicationContext(RedisHealthCheckInstance instance, String extractor) {
        super(instance, new InfoResultExtractor(extractor));
    }
}
