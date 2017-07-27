package com.ctrip.xpipe.redis.console.health.redismaster;

import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.redis.console.health.*;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import org.unidal.tuple.Pair;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author wenchao.meng
 *         <p>
 *         Apr 01, 2017
 */
@Component
@Lazy
public class DefaultRedisMasterMonitor extends BaseSampleMonitor<InstanceRedisMasterResult>{

    @Autowired
    private RedisMasterCollector redisMasterCollector;

    @Override
    protected void notifyCollectors(Sample sample) {
        redisMasterCollector.collect(sample);
    }

    @Override
    public void startSample(BaseSamplePlan<InstanceRedisMasterResult> rawPlan) throws SampleException {

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
    public Collection<BaseSamplePlan<InstanceRedisMasterResult>> generatePlan(List<DcMeta> dcMetas) {

        Map<Pair<String, String>, BaseSamplePlan<InstanceRedisMasterResult>> plans = new HashMap<>();

        for (DcMeta dcMeta : dcMetas) {
            for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {

                if(!isActiveDc(dcMeta.getId(), clusterMeta.getActiveDc())){
                    continue;
                }

                for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
                    Pair<String, String> cs = new Pair<>(clusterMeta.getId(), shardMeta.getId());
                    RedisMasterSamplePlan plan = (RedisMasterSamplePlan) plans.get(cs);
                    if (plan == null) {
                        plan = new RedisMasterSamplePlan(dcMeta.getId(), clusterMeta.getId(), shardMeta.getId(), shardMeta.getRedises());
                        plans.put(cs, plan);
                    }

                    for (RedisMeta redisMeta : shardMeta.getRedises()) {
                        plan.addRedis(dcMeta.getId(), redisMeta, new InstanceRedisMasterResult());
                    }
                }
            }
        }
        return plans.values();
    }

    @Override
    protected void addRedis(BaseSamplePlan<InstanceRedisMasterResult> plan, String dcId, RedisMeta redisMeta) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected BaseSamplePlan<InstanceRedisMasterResult> createPlan(String clusterId, String shardId) {
        throw new UnsupportedOperationException();
    }

    private boolean isActiveDc(String currentDc, String clusterActiveDc) {

        if(currentDc.equalsIgnoreCase(clusterActiveDc)){
            return true;
        }

        return false;
    }

}
