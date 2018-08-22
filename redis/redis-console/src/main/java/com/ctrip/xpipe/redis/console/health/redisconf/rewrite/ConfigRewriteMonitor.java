package com.ctrip.xpipe.redis.console.health.redisconf.rewrite;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.health.AbstractRedisConfMonitor;
import com.ctrip.xpipe.redis.console.health.BaseSamplePlan;
import com.ctrip.xpipe.redis.console.health.HealthCheckEndpoint;
import com.ctrip.xpipe.redis.console.health.Sample;
import com.google.common.collect.Lists;
import io.netty.util.internal.ConcurrentSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Set;

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
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList(ALERT_TYPE.REDIS_CONF_REWRITE_FAILURE);
    }

    @Override
    protected void doStartSample(BaseSamplePlan<InstanceRedisConfResult> plan) {

        long startNanoTime = recordSample(plan);
        sampleConfigRewrie(startNanoTime, plan);
    }

    private void sampleConfigRewrie(long startNanoTime, BaseSamplePlan<InstanceRedisConfResult> plan) {

        for (Map.Entry<HealthCheckEndpoint, InstanceRedisConfResult> entry : plan.getHostPort2SampleResult().entrySet()) {

            HealthCheckEndpoint endpoint = entry.getKey();
            try{
                findRedisSession(endpoint).configRewrite((result, th) -> {

                    if(th == null){
                        log.info("[sampleConfigRewrie][good]{}, {}", endpoint, result);
                        goodRedises.add(endpoint.getHostPort());
                        addInstanceSuccess(startNanoTime, endpoint, null);
                    }else{
                        log.info("[sampleConfigRewrie][bad]" + endpoint, th);
                        addInstanceFail(startNanoTime, endpoint, new ConfigRewriteFail("fail:" + endpoint.getHost(), th));
                    }
                });
            }catch (Exception e){
                addInstanceFail(startNanoTime, endpoint, e);
            }
        }
    }

    @Override
    protected BaseSamplePlan<InstanceRedisConfResult> createPlan(String dcId, String clusterId, String shardId) {

        return new RedisConfSamplePlan(clusterId, shardId);
    }

    @Override
    protected void addRedis(BaseSamplePlan<InstanceRedisConfResult> plan, String dcId, HealthCheckEndpoint endpoint) {

        HostPort hostPort = endpoint.getHostPort();

        if(goodRedises.contains(hostPort)){
            return;
        }

        log.debug("[addRedis]{}", hostPort);
        plan.addRedis(dcId, endpoint, new InstanceRedisConfResult());
    }


    public static class ConfigRewriteFail extends RedisConfFailException{

        public ConfigRewriteFail(String message, Throwable th) {
            super(message, th);
        }
    }
}
