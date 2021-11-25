package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinfostats;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;

public class CrdtInfoStatsContext extends AbstractActionContext<InfoResultExtractor, RedisHealthCheckInstance> {
    public CrdtInfoStatsContext(RedisHealthCheckInstance instance, InfoResultExtractor extractor) {
        super(instance, extractor);
    }
}
