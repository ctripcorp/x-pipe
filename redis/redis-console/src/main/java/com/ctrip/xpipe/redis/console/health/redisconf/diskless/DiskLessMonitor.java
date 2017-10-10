package com.ctrip.xpipe.redis.console.health.redisconf.diskless;

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
public class DiskLessMonitor extends AbstractRedisConfMonitor<DiskLessInstanceResult> {

    @Autowired
    private List<DiskLessCollector> collectors;

    public final static String REPL_DISKLESS_SYNC = "repl-diskless-sync";

    @Override
    protected void notifyCollectors(Sample<DiskLessInstanceResult> sample) {
        collectors.forEach(collector->collector.collect(sample));
    }

    @Override
    protected void doStartSample(BaseSamplePlan<DiskLessInstanceResult> plan) {
        long startNanoTime = recordSample(plan);
        sampleDiskLessOptionCheck(startNanoTime, plan);
    }

    private void sampleDiskLessOptionCheck(long startNanoTime, BaseSamplePlan<DiskLessInstanceResult> plan) {
        for (Map.Entry<HostPort, DiskLessInstanceResult> entry : plan.getHostPort2SampleResult().entrySet()) {

            HostPort hostPort = entry.getKey();
            try{
                RedisSession redisSession = findRedisSession(hostPort);
                redisSession.conf(REPL_DISKLESS_SYNC, new Callbackable<List<String>>() {
                    @Override
                    public void success(List<String> message) {
                        addInstanceSuccess(startNanoTime, hostPort, message);
                    }

                    @Override
                    public void fail(Throwable throwable) {
                        addInstanceFail(startNanoTime, hostPort, throwable);
                    }
                });
            }catch (Exception e){
                addInstanceFail(startNanoTime, hostPort.getHost(), hostPort.getPort(), e);
            }
        }
    }


    @Override
    protected void addRedis(BaseSamplePlan<DiskLessInstanceResult> plan, String dcId, RedisMeta redisMeta) {
        HostPort hostPort = new HostPort(redisMeta.getIp(), redisMeta.getPort());

        log.debug("[addRedis]{}", hostPort);
        plan.addRedis(dcId, redisMeta, new DiskLessInstanceResult());
    }

    @Override
    protected BaseSamplePlan<DiskLessInstanceResult> createPlan(String clusterId, String shardId) {
        return new DiskLessSamplePlan(clusterId, shardId);
    }

    @Override
    protected boolean addCluster(String dcName, ClusterMeta clusterMeta) {
        // check both primary and recovery site
        return true;
    }
}
