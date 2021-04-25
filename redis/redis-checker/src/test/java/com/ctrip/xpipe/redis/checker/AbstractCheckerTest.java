package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.checker.healthcheck.config.DefaultHealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultClusterInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import org.junit.BeforeClass;

/**
 * @author lishanglin
 * date 2021/3/9
 */
public class AbstractCheckerTest extends AbstractRedisTest {

    @BeforeClass
    public static void beforeAbstractCheckerTest(){
        System.setProperty(HealthChecker.ENABLED, "false");
    }

    protected RedisHealthCheckInstance newRandomRedisHealthCheckInstance(String currentDc, String activeDc, int port) throws Exception {
        RedisMeta redisMeta = newRandomFakeRedisMeta().setPort(port);
        DefaultRedisInstanceInfo info = new DefaultRedisInstanceInfo(currentDc,
                redisMeta.parent().parent().getId(), redisMeta.parent().getId(),
                new HostPort(redisMeta.getIp(), redisMeta.getPort()),
                activeDc, ClusterType.ONE_WAY);
        return newRandomRedisHealthCheckInstance(info);
    }

    protected RedisHealthCheckInstance newRandomRedisHealthCheckInstance(String currentDc, ClusterType clusterType, int port) throws Exception {
        RedisMeta redisMeta = newRandomFakeRedisMeta().setPort(port);
        DefaultRedisInstanceInfo info = new DefaultRedisInstanceInfo(currentDc,
                redisMeta.parent().parent().getId(), redisMeta.parent().getId(),
                new HostPort(redisMeta.getIp(), redisMeta.getPort()),
                null, clusterType);
        return newRandomRedisHealthCheckInstance(info);
    }

    protected RedisHealthCheckInstance newRandomRedisHealthCheckInstance(String activeDc, int port) throws Exception {
        RedisMeta redisMeta = newRandomFakeRedisMeta().setPort(port);
        DefaultRedisInstanceInfo info = new DefaultRedisInstanceInfo(redisMeta.parent().parent().parent().getId(),
                redisMeta.parent().parent().getId(), redisMeta.parent().getId(),
                new HostPort(redisMeta.getIp(), redisMeta.getPort()),
                activeDc, ClusterType.ONE_WAY);
        return newRandomRedisHealthCheckInstance(info);
    }

    protected RedisHealthCheckInstance newRandomRedisHealthCheckInstance(int port) throws Exception {
        RedisMeta redisMeta = newRandomFakeRedisMeta().setPort(port);
        DefaultRedisInstanceInfo info = new DefaultRedisInstanceInfo(redisMeta.parent().parent().parent().getId(),
                redisMeta.parent().parent().getId(), redisMeta.parent().getId(),
                new HostPort(redisMeta.getIp(), redisMeta.getPort()),
                redisMeta.parent().getActiveDc(), ClusterType.ONE_WAY);
        return newRandomRedisHealthCheckInstance(info);
    }

    protected RedisHealthCheckInstance newRandomRedisHealthCheckInstance(RedisInstanceInfo info) throws Exception {
        DefaultRedisHealthCheckInstance instance = new DefaultRedisHealthCheckInstance();
        instance.setInstanceInfo(info);
        instance.setEndpoint(new DefaultEndPoint(info.getHostPort().getHost(), info.getHostPort().getPort()));
        instance.setHealthCheckConfig(new DefaultHealthCheckConfig(buildCheckerConfig()));
        instance.setSession(new RedisSession(instance.getEndpoint(), scheduled, getXpipeNettyClientKeyedObjectPool()));
        return instance;
    }

    protected RedisHealthCheckInstance newHangedRedisHealthCheckInstance() throws Exception {
        RedisMeta redisMeta = newRandomFakeRedisMeta(getTimeoutIp(), 6379);
        DefaultRedisInstanceInfo info = new DefaultRedisInstanceInfo(redisMeta.parent().parent().parent().getId(),
                redisMeta.parent().parent().getId(), redisMeta.parent().getId(),
                new HostPort(redisMeta.getIp(), redisMeta.getPort()),
                redisMeta.parent().getActiveDc(), ClusterType.ONE_WAY);
        return newRandomRedisHealthCheckInstance(info);
    }

    protected ClusterHealthCheckInstance newRandomClusterHealthCheckInstance(String activeDc,ClusterType clusterType) throws Exception {
        DefaultClusterHealthCheckInstance instance = new DefaultClusterHealthCheckInstance();

        ClusterInstanceInfo info = new DefaultClusterInstanceInfo("cluster", activeDc,
                clusterType, 1);
        HealthCheckConfig config = new DefaultHealthCheckConfig(buildCheckerConfig());

        instance.setInstanceInfo(info).setHealthCheckConfig(config);

        return instance;
    }

    protected CheckerConfig buildCheckerConfig() {
        return new TestConfig();
    }

}
