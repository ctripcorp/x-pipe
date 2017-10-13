package com.ctrip.xpipe.redis.console.health.redisconf.rewrite;

import com.ctrip.xpipe.redis.console.health.BaseSamplePlan;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 13, 2017
 */
public class RedisConfSamplePlan extends BaseSamplePlan<InstanceRedisConfResult>{

    public RedisConfSamplePlan(String clusterId, String shardId) {
        super(clusterId, shardId);
    }
}
