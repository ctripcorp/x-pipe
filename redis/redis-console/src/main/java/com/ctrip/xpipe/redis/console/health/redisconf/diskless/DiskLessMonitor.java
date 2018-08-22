package com.ctrip.xpipe.redis.console.health.redisconf.diskless;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.health.*;
import com.ctrip.xpipe.redis.console.health.redisconf.Callbackable;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.google.common.collect.Lists;
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
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList(ALERT_TYPE.REDIS_REPL_DISKLESS_SYNC_ERROR);
    }

    @Override
    protected void doStartSample(BaseSamplePlan<DiskLessInstanceResult> plan) {
        long startNanoTime = recordSample(plan);
        sampleDiskLessOptionCheck(startNanoTime, plan);
    }

    private void sampleDiskLessOptionCheck(long startNanoTime, BaseSamplePlan<DiskLessInstanceResult> plan) {
        for (Map.Entry<HealthCheckEndpoint, DiskLessInstanceResult> entry : plan.getHostPort2SampleResult().entrySet()) {

            HealthCheckEndpoint endpoint = entry.getKey();
            try{
                RedisSession redisSession = findRedisSession(endpoint);
                redisSession.isDiskLessSync(new Callbackable<Boolean>() {
                    @Override
                    public void success(Boolean message) {
                        addInstanceSuccess(startNanoTime, endpoint, message);
                    }

                    @Override
                    public void fail(Throwable throwable) {
                        addInstanceFail(startNanoTime, endpoint, throwable);
                    }
                });
            }catch (Exception e){
                addInstanceFail(startNanoTime, endpoint, e);
            }
        }
    }


    @Override
    protected void addRedis(BaseSamplePlan<DiskLessInstanceResult> plan, String dcId, HealthCheckEndpoint endpoint) {
        log.debug("[addRedis]{}", endpoint.getHostPort());
        plan.addRedis(dcId, endpoint, new DiskLessInstanceResult());
    }

    @Override
    protected BaseSamplePlan<DiskLessInstanceResult> createPlan(String dcId, String clusterId, String shardId) {
        return new DiskLessSamplePlan(clusterId, shardId);
    }

    @Override
    protected boolean addCluster(String dcName, ClusterMeta clusterMeta) {
        // check both primary and recovery site
        return true;
    }
}
