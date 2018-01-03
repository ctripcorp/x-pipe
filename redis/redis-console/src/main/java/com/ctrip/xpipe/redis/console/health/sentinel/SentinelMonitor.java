package com.ctrip.xpipe.redis.console.health.sentinel;

import com.ctrip.xpipe.redis.console.config.ConsoleDbConfig;
import com.ctrip.xpipe.redis.console.health.AbstractRedisConfMonitor;
import com.ctrip.xpipe.redis.console.health.BaseSamplePlan;
import com.ctrip.xpipe.redis.console.health.RedisSession;
import com.ctrip.xpipe.redis.console.health.Sample;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 19, 2017
 */
@Component
@Lazy
public class SentinelMonitor extends AbstractRedisConfMonitor<InstanceSentinelResult> {

    public static final String helloChannel = "__sentinel__:hello";

    @Autowired
    private List<SentinelCollector> collectors;

    @Autowired
    private ConsoleDbConfig consoleDbConfig;

    @Autowired
    private ClusterService clusterService;

    @Override
    protected boolean shouldStart() {
        return consoleDbConfig.isSentinelAutoProcess();
    }

    @Override
    protected void notifyCollectors(Sample<InstanceSentinelResult> sample) {


        sample.getSamplePlan().getHostPort2SampleResult().keySet().forEach((hostPort) -> {

            RedisSession redisSession = findRedisSession(hostPort);
            redisSession.closeSubscribedChannel(helloChannel);
        });

        collectors.forEach((collector) -> collector.collect((SentinelSample) sample));

    }

    @Override
    protected void doStartSample(BaseSamplePlan<InstanceSentinelResult> plan) {

        long startNanoTime = recordSample(plan);
        sampleSentinel(startNanoTime, plan);
    }

    private void sampleSentinel(long startNanoTime, BaseSamplePlan<InstanceSentinelResult> plan) {

        log.debug("[sampleSentinel]{}, {}", plan.getClusterId(), plan.getShardId());

        plan.getHostPort2SampleResult().forEach((hostPort, instanceSentinelResult) -> {

            log.debug("[sampleSentinel]{}, {}, {}", plan.getClusterId(), plan.getShardId(), hostPort);

            RedisSession redisSession = findRedisSession(hostPort);

            redisSession.subscribeIfAbsent(helloChannel, new RedisSession.SubscribeCallback() {

                @Override
                public void message(String channel, String message) {

                    log.debug("[message]{},{}", hostPort, message);
                    SentinelHello hello = SentinelHello.fromString(message);
                    addInstanceSuccess(startNanoTime, hostPort, hello);
                }

                @Override
                public void fail(Exception e) {

                    addInstanceFail(startNanoTime, hostPort, e);
                    log.error("[fail]" + hostPort, e);
                }
            });


        });
    }

    @Override
    protected boolean addCluster(String dcName, ClusterMeta clusterMeta) {

        ClusterStatus clusterStatus = clusterService.clusterStatus(clusterMeta.getId());
        if(clusterStatus != ClusterStatus.Normal){
            log.info("[addCluster][false]{}, {}, {}", clusterMeta.getId(), dcName, clusterStatus);
            return false;
        }
        return true;
    }

    @Override
    protected void addRedis(BaseSamplePlan<InstanceSentinelResult> plan, String dcId, RedisMeta redisMeta) {
        plan.addRedis(dcId, redisMeta, new InstanceSentinelResult());
    }

    @Override
    protected BaseSamplePlan<InstanceSentinelResult> createPlan(String dcId, String clusterId, String shardId) {
        return new SentinelSamplePlan(clusterId, shardId);
    }


    @Override
    protected Sample<InstanceSentinelResult> createSample(long nanoTime, BaseSamplePlan<InstanceSentinelResult> plan) {

        return new SentinelSample(System.currentTimeMillis(),
                nanoTime,
                plan,
                5000);
    }
}

