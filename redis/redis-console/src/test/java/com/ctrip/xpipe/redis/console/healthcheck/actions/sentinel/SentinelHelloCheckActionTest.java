package com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleDbConfig;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionListener;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.*;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel.SentinelHelloCheckAction.HELLO_CHANNEL;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
public class SentinelHelloCheckActionTest extends AbstractConsoleTest {

    public static final String SUBSCRIBE_HEADER = "*3\r\n" +
            "$9\r\n" + "subscribe\r\n" +
            "$18\r\n" + "__sentinel__:hello\r\n" +
            ":1\r\n";

    public static final String SENTINEL_HELLO_TEMPLATE = "*3\r\n" +
            "$7\r\n" + "message\r\n" +
            "$18\r\n" + "__sentinel__:hello\r\n" +
            "$84\r\n" +
            "127.0.0.1,%d,d156c06308a5e5c6edba1f8786b32e22cfceafcc,8410,shard,127.0.0.1,16379,0\r\n";

    private SentinelHelloCheckAction action;

    @Mock
    private MetaCache metaCache;

    @Mock
    private ConsoleDbConfig config;

    private RedisHealthCheckInstance instance;

    private Server server;

    private Supplier<String> result;

    @SuppressWarnings("unchecked")
    @Before
    public void beforeSentinelHelloCheckActionTest() throws Exception {
        MockitoAnnotations.initMocks(this);
        server = startServerWithFlexibleResult(new Callable<String>() {
            @Override
            public String call() {
                if(result != null) {
                    return result.get();
                } else {
                    return "";
                }
            }
        });
        instance = newRandomRedisHealthCheckInstance("dc2", server.getPort());
        when(config.isSentinelAutoProcess()).thenReturn(true);
        action = new SentinelHelloCheckAction(scheduled, instance, executors, config);
    }

    @After
    public void afterSentinelHelloCheckActionTest() throws Exception {
        if(server != null) {
            server.stop();
        }
    }

    @Test
    public void testDoScheduledTaskWithProcessOff() {
        action = spy(action);
        when(config.isSentinelAutoProcess()).thenReturn(false);
        action.doTask();
        verify(action, never()).processSentinelHellos();
    }

    @Test
    public void testDoScheduledTaskWithRedisInPrimaryDc() {
        action = spy(action);
        when(metaCache.inBackupDc(any(HostPort.class))).thenReturn(false);
        action.doTask();
        verify(action, never()).processSentinelHellos();
    }

    @Test
    public void testDoScheduleTask() {
        action = spy(action);
        SentinelHelloCheckAction.SENTINEL_COLLECT_INFO_INTERVAL = 10;
        action.doTask();
        sleep(30);
        verify(action, times(1)).processSentinelHellos();
    }

    @Test
    public void testDoScheduleTaskWithSentinelHello() throws Exception {
        RedisInstanceInfo info = instance.getRedisInstanceInfo();
        logger.info("{}: {}", info.getActiveDc(), info.isInActiveDc());
        when(metaCache.getSentinelMonitorName(anyString(), anyString())).thenReturn(info.getShardId());
        when(metaCache.findMasterInSameShard(any(HostPort.class))).thenReturn(info.getHostPort());
        StringBuilder sb = new StringBuilder(SUBSCRIBE_HEADER);
        for(int i = 0; i < 5; i++) {
            sb.append(String.format(SENTINEL_HELLO_TEMPLATE, 5000 + i));
        }
        result = new Supplier<String>() {
            @Override
            public String get() {
                return sb.toString();
            }
        };
        AtomicInteger counter = new AtomicInteger(0);
        SentinelHelloCheckAction.SENTINEL_COLLECT_INFO_INTERVAL = 50;
        action.addListener(new SentinelHelloCollector() {

            @Override
            public void onAction(SentinelActionContext context) {
                Assert.assertEquals(5, context.getResult().size());
                counter.incrementAndGet();
            }

            @Override
            public void stopWatch(HealthCheckAction action) {

            }
        });
        Assert.assertEquals(1, action.getListeners().size());
        action.doTask();
        waitConditionUntilTimeOut(()->server.getConnected() > 0, 500);
        sleep(100);
        Assert.assertEquals(1, counter.get());
    }

    @Ignore
    @Test
    public void testSimulateSubscribe() {
        StringBuilder sb = new StringBuilder(SUBSCRIBE_HEADER);
        for(int i = 0; i < 5; i++) {
            sb.append(String.format(SENTINEL_HELLO_TEMPLATE, 5000 + i));
        }
        result = new Supplier<String>() {
            @Override
            public String get() {
                return sb.toString();
            }
        };

        action.getActionInstance().getRedisSession().subscribeIfAbsent(HELLO_CHANNEL, new RedisSession.SubscribeCallback() {
            @Override
            public void message(String channel, String message) {
                SentinelHello hello = SentinelHello.fromString(message);
                System.out.println(hello);
            }

            @Override
            public void fail(Throwable e) {
                logger.error("[sub-failed]", e);
            }
        });
        sleep(100);
    }

    @Test
    public void testProcessSentinelHellos() {
    }
}