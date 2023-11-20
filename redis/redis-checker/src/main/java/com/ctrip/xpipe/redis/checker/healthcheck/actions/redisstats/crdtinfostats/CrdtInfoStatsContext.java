package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinfostats;

import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractInfoContext;
import com.ctrip.xpipe.redis.core.protocal.cmd.CRDTInfoResultExtractor;

public class CrdtInfoStatsContext extends AbstractInfoContext<RedisHealthCheckInstance> {
    public CrdtInfoStatsContext(RedisHealthCheckInstance instance, String extractor) {
        super(instance, new CRDTInfoResultExtractor(extractor));
    }
}
