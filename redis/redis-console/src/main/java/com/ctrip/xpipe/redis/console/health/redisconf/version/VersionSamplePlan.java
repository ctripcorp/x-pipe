package com.ctrip.xpipe.redis.console.health.redisconf.version;

import com.ctrip.xpipe.redis.console.health.BaseSamplePlan;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

/**
 * @author chen.zhu
 * <p>
 * Sep 25, 2017
 */
public class VersionSamplePlan extends BaseSamplePlan<VersionInstanceResult> {
    public VersionSamplePlan(String clusterId, String shardId) {
        super(clusterId, shardId);
    }

    @Override
    public void addRedis(String dcId, RedisMeta redisMeta, VersionInstanceResult initSampleResult) {
        super.addRedis(dcId, redisMeta, initSampleResult);
    }
}
