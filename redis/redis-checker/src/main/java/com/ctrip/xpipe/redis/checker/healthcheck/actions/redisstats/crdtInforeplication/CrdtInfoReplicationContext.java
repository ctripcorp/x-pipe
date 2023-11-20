package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication;

import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractInfoContext;
import com.ctrip.xpipe.redis.core.protocal.cmd.CRDTInfoResultExtractor;

public class CrdtInfoReplicationContext extends AbstractInfoContext<RedisHealthCheckInstance> {
    public CrdtInfoReplicationContext(RedisHealthCheckInstance instance, String extractor) {
        super(instance, new CRDTInfoResultExtractor(extractor));
    }
}
