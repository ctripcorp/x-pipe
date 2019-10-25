package com.ctrip.xpipe.redis.console.healthcheck.actions.redismaster;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.ctrip.xpipe.simpleserver.Server;
import com.google.common.collect.Lists;
import org.junit.*;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.*;


/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
public class RedisMasterCheckActionTest extends AbstractConsoleTest {

    private static final String ROLE_MASTER = "*3\r\n" + "$6\r\n" + "master\r\n" + ":0\r\n" + "*0\r\n";

    private static final String ROLE_SLAVE = "*5\r\n" + "$5\r\n" + "slave\r\n" + "$9\r\n" +
            "127.0.0.1\r\n" + ":6379\r\n" + "$9\r\n" + "connected\r\n" + ":14\r\n";

    private RedisMasterCheckAction action;

    @Mock
    private MetaCache metaCache;

    @Mock
    private RedisService redisService;

    private RedisTbl redisTbl;

    private RedisHealthCheckInstance instance;

    private Server server;

    private Supplier<String> result;

    @SuppressWarnings("unchecked")
    @Before
    public void beforeRedisMasterCheckActionTest() throws Exception {
        MockitoAnnotations.initMocks(this);
        server = startServerWithFlexibleResult(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return result.get();
            }
        });
        instance = newRandomRedisHealthCheckInstance(server.getPort());
        RedisInstanceInfo info = instance.getRedisInstanceInfo();
        when(metaCache.inBackupDc(any(HostPort.class))).thenReturn(false);
        redisTbl = new RedisTbl().setRedisIp(info.getHostPort().getHost())
                .setRedisPort(info.getHostPort().getPort());
        when(redisService.findRedisesByDcClusterShard(info.getDcId(), info.getClusterId(), info.getShardId()))
                .thenReturn(Lists.newArrayList(redisTbl));
        doNothing().when(redisService).updateBatchMaster(anyList());
        action = new RedisMasterCheckAction(scheduled, instance, executors, redisService);
    }

    @After
    public void afterRedisMasterCheckActionTest() throws Exception {
        if(server != null) {
            server.stop();
        }
    }

    @Test
    public void testRedisMasterRoleCorrect() throws ResourceNotFoundException, TimeoutException {
        result = new Supplier<String>() {
            @Override
            public String get() {
                return ROLE_MASTER;
            }
        };
        instance.getRedisInstanceInfo().isMaster(true);
        action.doTask();
        waitConditionUntilTimeOut(()->server.getConnected() == 1, 200);
        waitConditionUntilTimeOut(()->instance.getRedisInstanceInfo().isMaster(), 50);
        Assert.assertTrue(instance.getRedisInstanceInfo().isMaster());
        verify(redisService, never()).updateBatchMaster(anyList());
        verify(redisService, never()).findRedisesByDcClusterShard(anyString(), anyString(), anyString());
    }

    @Test
    public void testRedisSlaveRoleCorrect() throws ResourceNotFoundException, TimeoutException {
        result = new Supplier<String>() {
            @Override
            public String get() {
                return ROLE_SLAVE;
            }
        };
        instance.getRedisInstanceInfo().isMaster(false);
        action.doTask();
        waitConditionUntilTimeOut(()->server.getConnected() == 1, 500);
        waitConditionUntilTimeOut(()->!instance.getRedisInstanceInfo().isMaster(), 500);
        Assert.assertFalse(instance.getRedisInstanceInfo().isMaster());
        verify(redisService, never()).updateBatchMaster(anyList());
        verify(redisService, never()).findRedisesByDcClusterShard(anyString(), anyString(), anyString());
    }

    @Test
    public void testRedisMasterRoleInCorrect() throws ResourceNotFoundException, TimeoutException {
        result = new Supplier<String>() {
            @Override
            public String get() {
                return ROLE_SLAVE;
            }
        };
        instance.getRedisInstanceInfo().isMaster(true);
        action.doTask();
        waitConditionUntilTimeOut(()->server.getConnected() == 1, 200);
        waitConditionUntilTimeOut(()->!instance.getRedisInstanceInfo().isMaster(), 200);
        Assert.assertFalse(instance.getRedisInstanceInfo().isMaster());
        verify(redisService, times(1)).updateBatchMaster(anyList());
        verify(redisService, times(1)).findRedisesByDcClusterShard(anyString(), anyString(), anyString());
    }

    @Test
    @Ignore //manually
    public void testRedisSlaveRoleInCorrect() throws Exception {
        result = new Supplier<String>() {
            @Override
            public String get() {
                return ROLE_MASTER;
            }
        };
        instance.getRedisInstanceInfo().isMaster(false);
        action.doTask();
        waitConditionUntilTimeOut(()->server.getConnected() == 1, 200);
        waitConditionUntilTimeOut(()->instance.getRedisInstanceInfo().isMaster(), 500);
        Assert.assertTrue(instance.getRedisInstanceInfo().isMaster());
        verify(redisService, times(1)).updateBatchMaster(anyList());
        verify(redisService, times(1)).findRedisesByDcClusterShard(anyString(), anyString(), anyString());
    }
}