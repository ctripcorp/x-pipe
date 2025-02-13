package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.RedisCheckRule;
import com.ctrip.xpipe.redis.checker.healthcheck.config.DefaultHealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.*;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import org.junit.BeforeClass;

import java.util.List;

/**
 * @author lishanglin
 * date 2021/3/9
 */
public class AbstractCheckerTest extends AbstractRedisTest {
    
    @BeforeClass
    public static void beforeAbstractCheckerTest(){
        System.setProperty(HealthChecker.ENABLED, "false");
        System.setProperty("DisableLoadProxyAgentJar", "true");
    }

    protected RedisHealthCheckInstance newRandomRedisHealthCheckInstance(String currentDc, String activeDc, int port) throws Exception {
        RedisMeta redisMeta = newRandomFakeRedisMeta().setPort(port);
        DefaultRedisInstanceInfo info = new DefaultRedisInstanceInfo(currentDc,
                ((ClusterMeta) redisMeta.parent().parent()).getId(), redisMeta.parent().getId(),
                new HostPort(redisMeta.getIp(), redisMeta.getPort()),
                activeDc, ClusterType.ONE_WAY);
        return newRandomRedisHealthCheckInstance(info);
    }

    protected RedisHealthCheckInstance newRandomRedisHealthCheckInstance(String currentDc, String activeDc, int port, ClusterType clusterType) throws Exception {
        RedisMeta redisMeta = newRandomFakeRedisMeta().setPort(port);
        DefaultRedisInstanceInfo info = new DefaultRedisInstanceInfo(currentDc,
                ((ClusterMeta) redisMeta.parent().parent()).getId(), redisMeta.parent().getId(),
                new HostPort(redisMeta.getIp(), redisMeta.getPort()),
                activeDc, clusterType);
        return newRandomRedisHealthCheckInstance(info);
    }

    protected RedisHealthCheckInstance newRandomRedisHealthCheckInstance(String currentDc, ClusterType clusterType, int port) throws Exception {
        RedisMeta redisMeta = newRandomFakeRedisMeta().setPort(port);
        DefaultRedisInstanceInfo info = new DefaultRedisInstanceInfo(currentDc,
                ((ClusterMeta) redisMeta.parent().parent()).getId(), redisMeta.parent().getId(),
                new HostPort(redisMeta.getIp(), redisMeta.getPort()),
                null, clusterType);
        return newRandomRedisHealthCheckInstance(info);
    }

    protected RedisHealthCheckInstance newRandomRedisHealthCheckInstance(String activeDc, int port) throws Exception {
        RedisMeta redisMeta = newRandomFakeRedisMeta().setPort(port);
        DefaultRedisInstanceInfo info = new DefaultRedisInstanceInfo(((ClusterMeta) redisMeta.parent().parent()).parent().getId(),
                ((ClusterMeta) redisMeta.parent().parent()).getId(), redisMeta.parent().getId(),
                new HostPort(redisMeta.getIp(), redisMeta.getPort()),
                activeDc, ClusterType.ONE_WAY);
        return newRandomRedisHealthCheckInstance(info);
    }

    protected RedisHealthCheckInstance newRandomRedisHealthCheckInstance(int port) throws Exception {
        return newRandomRedisHealthCheckInstance(port, null);
    }

    protected RedisHealthCheckInstance newRandomRedisHealthCheckInstance(int port, List<RedisCheckRule> redisCheckRules) throws Exception {
        RedisMeta redisMeta = newRandomFakeRedisMeta().setPort(port);
        DefaultRedisInstanceInfo info = new DefaultRedisInstanceInfo(((ClusterMeta) redisMeta.parent().parent()).parent().getId(),
                ((ClusterMeta) redisMeta.parent().parent()).getId(), redisMeta.parent().getId(),
                new HostPort(redisMeta.getIp(), redisMeta.getPort()),
                redisMeta.parent().getActiveDc(), ClusterType.ONE_WAY);
        if(null != redisCheckRules)
            info.setRedisCheckRules(redisCheckRules);

        return newRandomRedisHealthCheckInstance(info);
    }

    protected RedisHealthCheckInstance newRandomBiDirectionRedisHealthCheckInstance(int port, List<RedisCheckRule> redisCheckRules) throws Exception {
        RedisMeta redisMeta = newRandomFakeRedisMeta().setPort(port);
        DefaultRedisInstanceInfo info = new DefaultRedisInstanceInfo(((ClusterMeta) redisMeta.parent().parent()).parent().getId(),
                ((ClusterMeta) redisMeta.parent().parent()).getId(), redisMeta.parent().getId(),
                new HostPort(redisMeta.getIp(), redisMeta.getPort()),
                redisMeta.parent().getActiveDc(), ClusterType.BI_DIRECTION);
        if(null != redisCheckRules)
            info.setRedisCheckRules(redisCheckRules);

        return newRandomRedisHealthCheckInstance(info);
    }

    protected RedisHealthCheckInstance newRandomRedisHealthCheckInstance(RedisInstanceInfo info) throws Exception {
        DefaultRedisHealthCheckInstance instance = new DefaultRedisHealthCheckInstance();
        instance.setInstanceInfo(info);
        instance.setEndpoint(new DefaultEndPoint(info.getHostPort().getHost(), info.getHostPort().getPort()));
        instance.setHealthCheckConfig(new DefaultHealthCheckConfig(buildCheckerConfig(), buildDcRelationsService()));
        instance.setSession(new RedisSession(instance.getEndpoint(), scheduled, getXpipeNettyClientKeyedObjectPool(), buildCheckerConfig()));
        return instance;
    }

    protected RedisHealthCheckInstance newHangedRedisHealthCheckInstance() throws Exception {
        RedisMeta redisMeta = newRandomFakeRedisMeta(getTimeoutIp(), 6379);
        DefaultRedisInstanceInfo info = new DefaultRedisInstanceInfo(((ClusterMeta) redisMeta.parent().parent()).parent().getId(),
                ((ClusterMeta) redisMeta.parent().parent()).getId(), redisMeta.parent().getId(),
                new HostPort(redisMeta.getIp(), redisMeta.getPort()),
                redisMeta.parent().getActiveDc(), ClusterType.ONE_WAY);
        return newRandomRedisHealthCheckInstance(info);
    }

    protected ClusterHealthCheckInstance newRandomClusterHealthCheckInstance(String activeDc,ClusterType clusterType) throws Exception {
        DefaultClusterHealthCheckInstance instance = new DefaultClusterHealthCheckInstance();

        ClusterInstanceInfo info = new DefaultClusterInstanceInfo("cluster", activeDc,
                clusterType, 1);
        HealthCheckConfig config = new DefaultHealthCheckConfig(buildCheckerConfig(), buildDcRelationsService());

        instance.setInstanceInfo(info).setHealthCheckConfig(config);

        return instance;
    }

    protected CheckerConfig buildCheckerConfig() {
        return new TestConfig();
    }

    protected DcRelationsService buildDcRelationsService() {
        return new TestDcRelationsService();
    }

}
