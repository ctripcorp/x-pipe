package com.ctrip.xpipe.redis.console.health.redismaster;

import com.ctrip.xpipe.redis.console.health.BaseSamplePlan;
import com.ctrip.xpipe.redis.console.health.HealthCheckEndpoint;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

import java.util.LinkedList;
import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Apr 01, 2017
 */
public class RedisMasterSamplePlan extends BaseSamplePlan<InstanceRedisMasterResult>{

    private HealthCheckEndpoint masterEndpoint;
    private String dcName;

    private List<HealthCheckEndpoint> redises = new LinkedList<>();

    public RedisMasterSamplePlan(String dcName, String clusterId, String shardId) {
        super(clusterId, shardId);
        this.dcName = dcName;
    }

    public void addRedis(String dcId, HealthCheckEndpoint endpoint, InstanceRedisMasterResult result) {

        if (endpoint.getRedisMeta().isMaster()) {
            masterEndpoint = endpoint;
            super.addRedis(dcId, endpoint, result);
        }

        redises.add(endpoint);
    }

    public String getDcName() {
        return dcName;
    }

    public HealthCheckEndpoint getMasterEndpoint() {
        return masterEndpoint;
    }

    public List<HealthCheckEndpoint> getRediseEndpoints() {
        return redises;
    }

    @Override
    public boolean isEmpty() {
        return redises.isEmpty();
    }
}
