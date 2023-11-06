package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.infostats;

import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractInfoContext;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;

public class InfoStatsContext extends AbstractInfoContext<RedisHealthCheckInstance> {

    public InfoStatsContext(RedisHealthCheckInstance instance, String extractor) {
        super(instance, new InfoResultExtractor(extractor));
    }
}
