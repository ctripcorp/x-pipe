package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.checker.model.RedisMsg;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static com.ctrip.xpipe.cluster.ClusterType.ONE_WAY;
import static com.ctrip.xpipe.cluster.ClusterType.SINGLE_DC;

@RunWith(MockitoJUnitRunner.class)
public class RedisMsgCollectorTest {

    private RedisMsgCollector redisMsgCollector;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        redisMsgCollector = new RedisMsgCollector();
    }

    @Test
    public void testOnAction() {
        RawInfoActionContext context = Mockito.mock(RawInfoActionContext.class);
        RedisInstanceInfo info = Mockito.mock(RedisInstanceInfo.class);
        HostPort hostPort = new HostPort("127.0.0.1", 6379);
        DcClusterShard dcClusterShard = new DcClusterShard("dc1", "cluster1", "shard1");
        RedisMsg redisMsg = new RedisMsg(1, 1024, 1000);

        Map<HostPort, RedisMsg> initialMap = new HashMap<>();
        initialMap.put(hostPort, redisMsg);

        Mockito.when(context.getResult()).thenReturn("used_memory:2048\nmaster_repl_offset:2000\nmaxmemory:1024\nswap_used_db_size:1024\n");
        RedisHealthCheckInstance actionInstance =  Mockito.mock(RedisHealthCheckInstance.class);
        HealthCheckConfig healthCheckConfig = Mockito.mock(HealthCheckConfig.class);

        Mockito.when(context.instance()).thenReturn(actionInstance);
        Mockito.when(actionInstance.getCheckInfo()).thenReturn(info);

        redisMsgCollector.onAction(context);

        Mockito.when(context.getResult()).thenReturn(null);
        redisMsgCollector.onAction(context);

    }

    @Test
    public void testStopWatch() {
        HealthCheckAction action = Mockito.mock(HealthCheckAction.class);
        DefaultRedisInstanceInfo info = Mockito.mock(DefaultRedisInstanceInfo.class);
        HostPort hostPort = new HostPort("127.0.0.1", 6379);
        DcClusterShard dcClusterShard = new DcClusterShard("dc1", "cluster1", "shard1");
        RedisMsg redisMsg = new RedisMsg(1, 1024, 1000);

        Map<HostPort, RedisMsg> initialMap = new HashMap<>();
        initialMap.put(hostPort, redisMsg);
        HealthCheckInstance actionInstance =  Mockito.mock(HealthCheckInstance.class);

        Mockito.when(action.getActionInstance()).thenReturn(actionInstance);
        Mockito.when(actionInstance.getCheckInfo()).thenReturn(info);
        Mockito.when(info.getDcId()).thenReturn(dcClusterShard.getDcId());
        Mockito.when(info.getClusterId()).thenReturn(dcClusterShard.getClusterId());
        Mockito.when(info.getShardId()).thenReturn(dcClusterShard.getShardId());
        Mockito.when(info.getHostPort()).thenReturn(hostPort);

        redisMsgCollector.stopWatch(action);

    }

}