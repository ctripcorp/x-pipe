package com.ctrip.xpipe.redis.console.health.migration.diskless;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.health.*;
import com.ctrip.xpipe.redis.console.health.migration.Callbackable;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.utils.ObjectUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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

    private ConcurrentMap<HostPort, RedisInfoAndConf> redisInfoAndConfMap = new ConcurrentHashMap<>();

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
                RedisInfoAndConf redisInfoAndConf = findRedisInfoAndConf(hostPort);
                redisSession.conf(REPL_DISKLESS_SYNC, new Callbackable<List<String>>() {
                    @Override
                    public void success(List<String> message) {
                        redisInfoAndConf.setServerConf(message);
                        redisSession.serverInfo(new Callbackable<String>() {
                            @Override
                            public void success(String message) {
                                redisInfoAndConf.setServerInfo(message);
                                addInstanceSuccess(startNanoTime, hostPort, redisInfoAndConf);
                            }

                            @Override
                            public void fail(Throwable throwable) {
                                addInstanceFail(startNanoTime, hostPort, throwable);
                            }
                        });

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

    private RedisInfoAndConf findRedisInfoAndConf(HostPort hostPort) {
        redisInfoAndConfMap.putIfAbsent(hostPort, new RedisInfoAndConf());
        return redisInfoAndConfMap.get(hostPort);
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
        String activeDC = clusterMeta.getActiveDc();
        return ObjectUtils.equals(activeDC, dcName);
    }
}
