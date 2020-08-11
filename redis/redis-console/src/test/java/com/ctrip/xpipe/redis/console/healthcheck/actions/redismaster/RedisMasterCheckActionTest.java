package com.ctrip.xpipe.redis.console.healthcheck.actions.redismaster;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.healthcheck.*;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.*;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

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
            context = invocation.getArgumentAt(0, RedisMasterActionContext.class);
            return null;
        }).when(listener).onAction(Mockito.any());

        server = startServerWithFlexibleResult(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return result.get();
            }
        });
        instance = newRandomRedisHealthCheckInstance(server.getPort());
        RedisInstanceInfo info = instance.getRedisInstanceInfo();
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
        instance.getRedisInstanceInfo().isMaster(false);
        action.doTask();

        waitConditionUntilTimeOut(() -> null != context, 1000);
        Assert.assertFalse(context.instance().getRedisInstanceInfo().isMaster());
        Assert.assertEquals(com.ctrip.xpipe.api.server.Server.SERVER_ROLE.MASTER, context.getResult());
    }

    @Test
    public void testRedisSlave() throws Exception {
        result = new Supplier<String>() {
            @Override
            public String get() {
                return ROLE_SLAVE;
            }
        };
        instance.getRedisInstanceInfo().isMaster(true);
        action.doTask();

        waitConditionUntilTimeOut(() -> null != context, 1000);
        Assert.assertTrue(context.instance().getRedisInstanceInfo().isMaster());
        Assert.assertEquals(com.ctrip.xpipe.api.server.Server.SERVER_ROLE.SLAVE, context.getResult());
    }

    @Test
    public void testRedisHang() throws Exception {
        instance = newHangedRedisHealthCheckInstance();
        instance.getRedisInstanceInfo().isMaster(true);
        action = new RedisMasterCheckAction(scheduled, instance, executors);
        action.addListener(listener);
        action.doTask();

        waitConditionUntilTimeOut(() -> null != context, 1000);
        Assert.assertTrue(context.instance().getRedisInstanceInfo().isMaster());
        Assert.assertEquals(com.ctrip.xpipe.api.server.Server.SERVER_ROLE.UNKNOWN, context.getResult());
    }
}