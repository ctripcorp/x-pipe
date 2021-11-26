package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractInfoContext;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;

public class CrdtInfoReplicationContext extends AbstractInfoContext {
    public CrdtInfoReplicationContext(RedisHealthCheckInstance instance, InfoResultExtractor extractor) {
        super(instance, extractor);
    }
}
