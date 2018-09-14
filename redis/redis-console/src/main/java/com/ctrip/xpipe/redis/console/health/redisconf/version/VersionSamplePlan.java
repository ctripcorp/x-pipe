package com.ctrip.xpipe.redis.console.health.redisconf.version;

import com.ctrip.xpipe.redis.console.health.BaseSamplePlan;

/**
 * @author chen.zhu
 * <p>
 * Sep 25, 2017
 */
public class VersionSamplePlan extends BaseSamplePlan<VersionInstanceResult> {
    public VersionSamplePlan(String clusterId, String shardId) {
        super(clusterId, shardId);
    }
}
