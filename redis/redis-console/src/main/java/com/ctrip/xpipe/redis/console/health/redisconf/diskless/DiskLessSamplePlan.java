package com.ctrip.xpipe.redis.console.health.redisconf.diskless;

import com.ctrip.xpipe.redis.console.health.BaseSamplePlan;

/**
 * @author chen.zhu
 * <p>
 * Sep 25, 2017
 */
public class DiskLessSamplePlan extends BaseSamplePlan<DiskLessInstanceResult> {
    public DiskLessSamplePlan(String clusterId, String shardId) {
        super(clusterId, shardId);
    }
}
