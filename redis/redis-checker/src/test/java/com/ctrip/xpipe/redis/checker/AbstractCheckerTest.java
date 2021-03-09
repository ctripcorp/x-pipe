package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthChecker;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DcClusterDelayMarkDown;
import com.ctrip.xpipe.redis.checker.healthcheck.config.DefaultHealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;
import org.junit.BeforeClass;

import java.util.Set;

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

    protected CheckerConfig buildCheckerConfig() {
        return new CheckerConfig() {
            @Override
            public int getRedisReplicationHealthCheckInterval() {
                return 0;
            }

            @Override
            public int getClusterHealthCheckInterval() {
                return 0;
            }

            @Override
            public int getDownAfterCheckNums() {
                return 0;
            }

            @Override
            public int getDownAfterCheckNumsThroughProxy() {
                return 0;
            }

            @Override
            public int getRedisConfCheckIntervalMilli() {
                return 0;
            }

            @Override
            public int getSentinelCheckIntervalMilli() {
                return 0;
            }

            @Override
            public int getHealthyDelayMilli() {
                return 0;
            }

            @Override
            public int getHealthyDelayMilliThroughProxy() {
                return 0;
            }

            @Override
            public String getReplDisklessMinRedisVersion() {
                return null;
            }

            @Override
            public String getXRedisMinimumRequestVersion() {
                return null;
            }

            @Override
            public int getPingDownAfterMilli() {
                return 0;
            }

            @Override
            public int getPingDownAfterMilliThroughProxy() {
                return 0;
            }

            @Override
            public int getSentinelRateLimitSize() {
                return 0;
            }

            @Override
            public boolean isSentinelRateLimitOpen() {
                return false;
            }

            @Override
            public QuorumConfig getDefaultSentinelQuorumConfig() {
                return null;
            }

            @Override
            public Set<DcClusterDelayMarkDown> getDelayedMarkDownDcClusters() {
                return null;
            }

            @Override
            public boolean isConsoleSiteUnstable() {
                return false;
            }

            @Override
            public int getQuorum() {
                return 0;
            }

            @Override
            public Set<String> getIgnoredHealthCheckDc() {
                return null;
            }
        };
    }

}
