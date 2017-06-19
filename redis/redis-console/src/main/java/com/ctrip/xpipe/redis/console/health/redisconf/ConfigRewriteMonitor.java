package com.ctrip.xpipe.redis.console.health.redisconf;

import com.ctrip.xpipe.metric.HostPort;
import com.ctrip.xpipe.redis.console.health.AbstractRedisConfMonitor;
import com.ctrip.xpipe.redis.console.health.BaseSamplePlan;
import com.ctrip.xpipe.redis.console.health.Sample;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import io.netty.util.internal.ConcurrentSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 13, 2017
 */
@Component
@Lazy
public class ConfigRewriteMonitor extends AbstractRedisConfMonitor<InstanceRedisConfResult> {

    private  Set<HostPort> goodRedises = new ConcurrentSet<>();

    @Autowired
    private List<RedisConfCollector> collectors;

    @Override
    protected void notifyCollectors(Sample<InstanceRedisConfResult> sample) {

        collectors.forEach((collector) -> collector.collect(sample));
    }


    @Override
    protected void doStartSample(BaseSamplePlan<InstanceRedisConfResult> plan) {

        long startNanoTime = recordSample(plan);
        sampleConfigRewrie(startNanoTime, plan);
    }

    private void sampleConfigRewrie(long startNanoTime, BaseSamplePlan<InstanceRedisConfResult> plan) {

        for (Map.Entry<HostPort, InstanceRedisConfResult> entry : plan.getHostPort2SampleResult().entrySet()) {

            HostPort hostPort = entry.getKey();
            try{
                findRedisSession(hostPort).configRewrite((result, th) -> {

                    if(th == null){
                        log.info("[sampleConfigRewrie][good]{}, {}", hostPort, result);
                        goodRedises.add(hostPort);
                        addInstanceSuccess(startNanoTime, hostPort, null);
                    }else{
                        log.info("[sampleConfigRewrie][bad]" + hostPort, th);
                        addInstanceFail(startNanoTime, hostPort.getHost(), hostPort.getPort(),
                                new ConfigRewriteFail("fail:" + hostPort, th));
                    }
                });
            }catch (Exception e){
                addInstanceFail(startNanoTime, hostPort.getHost(), hostPort.getPort(), e);
            }
        }
    }

    @Override
    protected BaseSamplePlan<InstanceRedisConfResult> createPlan(String clusterId, String shardId) {

        return new RedisConfSamplePlan(clusterId, shardId);
    }

    @Override
    protected void addRedis(BaseSamplePlan<InstanceRedisConfResult> plan, String dcId, RedisMeta redisMeta) {

        HostPort hostPort = new HostPort(redisMeta.getIp(), redisMeta.getPort());

        if(goodRedises.contains(hostPort)){
            return;
        }

        log.debug("[addRedis]{}", hostPort);
        plan.addRedis(dcId, redisMeta, new InstanceRedisConfResult());
    }


    public static class ConfigRewriteFail extends RedisConfFailException{

        public ConfigRewriteFail(String message, Throwable th) {
            super(message, th);
        }
    }
}
