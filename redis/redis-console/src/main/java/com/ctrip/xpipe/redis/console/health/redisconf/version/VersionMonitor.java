package com.ctrip.xpipe.redis.console.health.redisconf.version;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.health.*;
import com.ctrip.xpipe.redis.console.health.redisconf.Callbackable;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * Sep 25, 2017
 */

@Component
@Lazy
@ConditionalOnProperty(name = { HealthChecker.ENABLED }, matchIfMissing = true)
public class VersionMonitor extends AbstractRedisConfMonitor<VersionInstanceResult> {

    @Autowired
    private List<VersionCollector> collectors;

    @Override
    protected void notifyCollectors(Sample<VersionInstanceResult> sample) {
        collectors.forEach(collector->collector.collect(sample));
    }

    @Override
    protected void doStartSample(BaseSamplePlan<VersionInstanceResult> plan) {
        long startNanoTime = recordSample(plan);
        sampleVersionCheck(startNanoTime, plan);
    }

    private void sampleVersionCheck(long startNanoTime, BaseSamplePlan<VersionInstanceResult> plan) {
        for (Map.Entry<HostPort, VersionInstanceResult> entry : plan.getHostPort2SampleResult().entrySet()) {

            HostPort hostPort = entry.getKey();
            try{
                RedisSession redisSession = findRedisSession(hostPort);
                redisSession.infoServer(new Callbackable<String>() {
                    @Override
                    public void success(String message) {
                        addInstanceSuccess(startNanoTime, hostPort.getHost(), hostPort.getPort(), message);
                    }

                    @Override
                    public void fail(Throwable throwable) {
                        addInstanceFail(startNanoTime, hostPort.getHost(), hostPort.getPort(), throwable);
                    }
                });
            }catch (Exception e){
                addInstanceFail(startNanoTime, hostPort.getHost(), hostPort.getPort(), e);
            }
        }
    }

    @Override
    protected BaseSamplePlan<VersionInstanceResult> createPlan(String dcId, String clusterId, String shardId) {
        return new VersionSamplePlan(clusterId, shardId);
    }

    @Override
    protected void addRedis(BaseSamplePlan<VersionInstanceResult> plan, String dcId, RedisMeta redisMeta) {
        HostPort hostPort = new HostPort(redisMeta.getIp(), redisMeta.getPort());

        log.debug("[addRedis]{}", hostPort);
        plan.addRedis(dcId, redisMeta, new VersionInstanceResult());
    }


    @Override
    protected boolean addCluster(String dcName, ClusterMeta clusterMeta) {
        return true;
    }
}
