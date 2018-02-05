package com.ctrip.xpipe.redis.console.health.redisconf.backlog;

import com.ctrip.xpipe.redis.console.health.BaseSamplePlan;

/**
 * @author chen.zhu
 * <p>
 * Feb 05, 2018
 */
public class BacklogActiveSamplePlan extends BaseSamplePlan<InstanceInfoReplicationResult> {

    public BacklogActiveSamplePlan(String clusterId, String shardId) {
        super(clusterId, shardId);
    }
}
