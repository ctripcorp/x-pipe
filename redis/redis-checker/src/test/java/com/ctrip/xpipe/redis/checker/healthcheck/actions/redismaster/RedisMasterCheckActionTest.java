package com.ctrip.xpipe.redis.checker.healthcheck.actions.redismaster;

import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
public class RedisMasterCheckActionTest extends AbstractCheckerTest {

    private static final String ROLE_MASTER = "*3\r\n" + "$6\r\n" + "master\r\n" + ":0\r\n" + "*0\r\n";

    private static final String ROLE_SLAVE = "*5\r\n" + "$5\r\n" + "slave\r\n" + "$9\r\n" +
            "127.0.0.1\r\n" + ":6379\r\n" + "$9\r\n" + "connected\r\n" + ":14\r\n";

    private RedisMasterCheckAction action;

    @Mock
    private RedisMasterActionListener listener;

    private RedisHealthCheckInstance instance;

    private Server server;

    private Supplier<String> result;

    private RedisMasterActionContext context = null;

    @SuppressWarnings("unchecked")
    @Before
    public void beforeRedisMasterCheckActionTest() throws Exception {
        MockitoAnnotations.initMocks(this);
        Mockito.when(listener.worksfor(Mockito.any())).thenReturn(true);
        Mockito.doAnswer(invocation -> {
            context = invocation.getArgument(0, RedisMasterActionContext.class);
            return null;
        }).when(listener).onAction(Mockito.any());

        server = startServerWithFlexibleResult(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return result.get();
            }
        });
        instance = newRandomRedisHealthCheckInstance(server.getPort());
        RedisInstanceInfo info = instance.getCheckInfo();
        action = new RedisMasterCheckAction(scheduled, instance, executors);
        action.addListener(listener);
    }

    @After
    public void afterRedisMasterCheckActionTest() throws Exception {
        if(server != null) {
            server.stop();
        }
    }

    @Test
    public void testRedisMaster() throws Exception {
        result = new Supplier<String>() {
            @Override
            public String get() {
                return ROLE_MASTER;
            }
        };
        instance.getCheckInfo().isMaster(false);
        action.doTask();

        waitConditionUntilTimeOut(() -> null != context, 1000);
        Assert.assertFalse(context.instance().getCheckInfo().isMaster());
        Assert.assertEquals(com.ctrip.xpipe.api.server.Server.SERVER_ROLE.MASTER, context.getResult().getServerRole());
    }

    @Test
    public void testRedisSlave() throws Exception {
        result = new Supplier<String>() {
            @Override
            public String get() {
                return ROLE_SLAVE;
            }
        };
        instance.getCheckInfo().isMaster(true);
        action.doTask();

        waitConditionUntilTimeOut(() -> null != context, 1000);
        Assert.assertTrue(context.instance().getCheckInfo().isMaster());
        Assert.assertEquals(com.ctrip.xpipe.api.server.Server.SERVER_ROLE.SLAVE, context.getResult().getServerRole());
    }

    @Test
    public void testRedisHang() throws Exception {
        instance = newHangedRedisHealthCheckInstance();
        instance.getCheckInfo().isMaster(true);
        action = new RedisMasterCheckAction(scheduled, instance, executors);
        action.addListener(listener);
        action.doTask();

        waitConditionUntilTimeOut(() -> null != context, 1000);
        Assert.assertTrue(context.instance().getCheckInfo().isMaster());
        Assert.assertFalse(context.isSuccess());
    }
}