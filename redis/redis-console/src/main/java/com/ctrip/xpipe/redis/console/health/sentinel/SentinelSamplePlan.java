package com.ctrip.xpipe.redis.console.health.sentinel;

import com.ctrip.xpipe.redis.console.health.BaseSamplePlan;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 19, 2017
 */
public class SentinelSamplePlan extends BaseSamplePlan<InstanceSentinelResult>{


    public SentinelSamplePlan(String clusterId, String shardId) {
        super(clusterId, shardId);
    }

    @Override
    public void addRedis(String dcId, RedisMeta redisMeta, InstanceSentinelResult initSampleResult) {

        if(redisMeta.parent().getActiveDc().equalsIgnoreCase(dcId)){
            return;
        }
        super.addRedis(dcId, redisMeta, initSampleResult);
    }
}
