package com.ctrip.xpipe.redis.console.health.redismaster;

import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.redis.console.health.*;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * @author wenchao.meng
 *         <p>
 *         Apr 01, 2017
 */
@Component
@Lazy
public class DefaultRedisMasterMonitor extends AbstractRedisConfMonitor<InstanceRedisMasterResult>{

    @Autowired
    private RedisMasterCollector redisMasterCollector;

    @Override
    protected void notifyCollectors(Sample sample) {
        redisMasterCollector.collect(sample);
    }

    @Override
    protected void doStartSample(BaseSamplePlan<InstanceRedisMasterResult> rawPlan) {
        RedisMasterSamplePlan plan = (RedisMasterSamplePlan) rawPlan;
        long startNanoTime = recordSample(plan);
        try{
            sampleRole(startNanoTime, plan);
        }catch (Exception e){
            addInstanceSuccess(startNanoTime, plan.getMasterHost(), plan.getMasterPort(), Server.SERVER_ROLE.UNKNOWN.toString());
            log.error("[startSample]" + plan, e);
        }

    }

    private void sampleRole(final long startNanoTime, RedisMasterSamplePlan plan) {

        RedisSession session = findRedisSession(plan.getMasterHost(), plan.getMasterPort());

        session.role(new RedisSession.RollCallback() {

            @Override
            public void role(String role) {
                addInstanceSuccess(startNanoTime, plan.getMasterHost(), plan.getMasterPort(), role);
            }

            @Override
            public void fail(Throwable e) {
                addInstanceSuccess(startNanoTime, plan.getMasterHost(), plan.getMasterPort(), Server.SERVER_ROLE.UNKNOWN.toString());
            }
        });
    }

    @Override
    protected boolean addCluster(String dcName, ClusterMeta clusterMeta) {
        return isActiveDc(dcName, clusterMeta.getActiveDc());
    }

    @Override
    protected void addRedis(BaseSamplePlan<InstanceRedisMasterResult> plan, String dcId, RedisMeta redisMeta) {
        plan.addRedis(dcId, redisMeta, new InstanceRedisMasterResult());
    }

    @Override
    protected BaseSamplePlan<InstanceRedisMasterResult> createPlan(String dcId, String clusterId, String shardId) {
        return new RedisMasterSamplePlan(dcId, clusterId, shardId);
    }

    private boolean isActiveDc(String currentDc, String clusterActiveDc) {

        if(currentDc.equalsIgnoreCase(clusterActiveDc)){
            return true;
        }

        return false;
    }

}
