package com.ctrip.xpipe.redis.console.health.redisconf.backlog;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.health.*;
import com.ctrip.xpipe.redis.console.health.redisconf.Callbackable;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Feb 05, 2018
 */
@Component
@Lazy
@ConditionalOnProperty(name = { HealthChecker.ENABLED }, matchIfMissing = true)
public class BacklogActiveMonitor  extends AbstractRedisConfMonitor<InstanceInfoReplicationResult> {

    @Autowired
    private List<BacklogActiveCollector> collectors;

    @Autowired
    private RedisSessionManager sessionManager;

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList(ALERT_TYPE.REPL_BACKLOG_NOT_ACTIVE);
    }

    @Override
    protected void doStartSample(BaseSamplePlan<InstanceInfoReplicationResult> plan) {
        long recordTime = recordSample(plan);
        sampleInfoReplication(recordTime, plan);
    }

    public void sampleInfoReplication(long recordTime, BaseSamplePlan<InstanceInfoReplicationResult> plan) {
        for(HealthCheckEndpoint endpoint : plan.getHostPort2SampleResult().keySet()) {
            RedisSession session = sessionManager.findOrCreateSession(endpoint);
            session.infoReplication(new Callbackable<String>() {
                @Override
                public void success(String message) {
                    addInstanceSuccess(recordTime, endpoint, message);
                }

                @Override
                public void fail(Throwable throwable) {
                    addInstanceFail(recordTime, endpoint, throwable);
                }
            });
        }
    }

    @Override
    protected void notifyCollectors(Sample<InstanceInfoReplicationResult> sample) {
        for(BacklogActiveCollector collector : collectors) {
            try {
                collector.collect(sample);
            } catch (Exception e) {
                log.error("[notifyCollectors]", e);
            }
        }
    }

    @Override
    protected void addRedis(BaseSamplePlan<InstanceInfoReplicationResult> plan, String dcId, HealthCheckEndpoint endpoint) {
        // check slave only
        if(endpoint.getRedisMeta().isMaster()) {
            return;
        }
        plan.addRedis(dcId, endpoint, new InstanceInfoReplicationResult());
    }

    @Override
    protected BaseSamplePlan<InstanceInfoReplicationResult> createPlan(String dcId, String clusterId, String shardId) {
        return new BacklogActiveSamplePlan(clusterId, shardId);
    }
}
