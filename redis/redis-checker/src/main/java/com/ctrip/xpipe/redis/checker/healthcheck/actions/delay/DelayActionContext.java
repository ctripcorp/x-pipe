package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.cluster.DcGroupType;
import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStatus;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public class DelayActionContext extends AbstractActionContext<Long, RedisHealthCheckInstance> {

    private Map<Long, Long> upstreamShardsDelay = new ConcurrentHashMap<>();


    public DelayActionContext(RedisHealthCheckInstance instance, Long delay) {
        super(instance, delay);
    }

    public DelayActionContext(RedisHealthCheckInstance instance, Long shardId, Long delay) {
        super(instance, HealthStatus.UNSET_TIME);
        RedisInstanceInfo instanceInfo = instance.getCheckInfo();
        if (instanceInfo.getClusterType().equals(ClusterType.ONE_WAY) && !DcGroupType.isNullOrDrMaster(instanceInfo.getDcGroupType())) {
            upstreamShardsDelay.put(shardId, delay);
        }
    }

    public Map<Long, Long> getUpstreamShardsDelay() {
        return upstreamShardsDelay;
    }

    public void addUpstreamShardDelay(Long shardId, Long delay) {
        this.upstreamShardsDelay.put(shardId, delay);
    }

}
