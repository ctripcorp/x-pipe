package com.ctrip.xpipe.redis.console.health.redismaster;

import com.ctrip.xpipe.redis.console.health.BaseSamplePlan;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Apr 01, 2017
 */
public class RedisMasterSamplePlan extends BaseSamplePlan<InstanceRedisMasterResult>{

    private String masterHost;
    private int masterPort;
    private String dcName;

    private List<RedisMeta> redises;

    public RedisMasterSamplePlan(String dcName, String clusterId, String shardId, List<RedisMeta> redises) {
        super(clusterId, shardId);
        this.dcName = dcName;
        this.redises = redises;
    }

    public void addRedis(String dcId, RedisMeta redisMeta, InstanceRedisMasterResult result) {

        if (redisMeta.isMaster()) {
            masterHost = redisMeta.getIp();
            masterPort = redisMeta.getPort();
            super.addRedis(dcId, redisMeta, result);
        }

    }

    public String getDcName() {
        return dcName;
    }

    public int getMasterPort() {
        return masterPort;
    }

    public String getMasterHost() {
        return masterHost;
    }

    public List<RedisMeta> getRedises() {
        return redises;
    }
}
