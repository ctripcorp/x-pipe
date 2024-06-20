package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.info;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Created by yu
 * 2023/8/29
 */
@RunWith(MockitoJUnitRunner.class)
public class RedisInfoActionTest extends AbstractCheckerTest {
    private RedisHealthCheckInstance instance;

    private Server redis;

    private RedisInfoAction action;

    private AtomicInteger redisCallCnt = new AtomicInteger(0);

    private AtomicInteger listenerCallCnt = new AtomicInteger(0);

    AtomicReference<ActionContext> contextRef = new AtomicReference();

    @Mock
    private CheckerDbConfig checkerDbConfig;

    private static final String INFO_RESPONSE = "# Memory\n" +
            "used_memory:550515888\n" +
            "used_memory_human:525.01M\n" +
            "used_memory_rss:544837632\n" +
            "used_memory_rss_human:519.60M\n" +
            "used_memory_peak:550862048\n" +
            "used_memory_peak_human:525.34M\n" +
            "used_memory_peak_perc:99.94%\n" +
            "used_memory_overhead:212334872\n" +
            "used_memory_startup:803592\n" +
            "used_memory_dataset:338181016\n" +
            "used_memory_dataset_perc:61.52%\n" +
            "allocator_allocated:550579680\n" +
            "allocator_active:551002112\n" +
            "allocator_resident:560062464\n" +
            "total_system_memory:8201191424\n" +
            "total_system_memory_human:7.64G\n" +
            "used_memory_lua:45056\n" +
            "used_memory_lua_human:44.00K\n" +
            "used_memory_scripts:816\n" +
            "used_memory_scripts_human:816B\n" +
            "number_of_cached_scripts:2";

    @Before
    public void beforeInfoStatsActionTest() throws Exception {
        redis = startServer(randomPort(), new Function<String, String>() {
            @Override
            public String apply(String s) {
                redisCallCnt.incrementAndGet();

                if (s.trim().toLowerCase().startsWith("info")) {
                    return  String.format("$%d\r\n%s\r\n", INFO_RESPONSE.length(), INFO_RESPONSE);
                } else {
                    return "+OK\r\n";
                }
            }
        });

        instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.ONE_WAY, redis.getPort());
        Mockito.when(checkerDbConfig.isKeeperBalanceInfoCollectOn()).thenReturn(true);
        action = new RedisInfoAction(scheduled, instance, executors, checkerDbConfig);

        action.addListener(new HealthCheckActionListener() {
            @Override
            public void onAction(ActionContext context) {
                logger.info("[onAction]{}", context);
                listenerCallCnt.incrementAndGet();
                contextRef.set(context);
            }

            @Override
            public boolean worksfor(ActionContext t) {
                return true;
            }

            @Override
            public void stopWatch(HealthCheckAction action) {

            }
        });
    }

    @Test
    public void testMaster() throws TimeoutException {
        AbstractHealthCheckAction.ScheduledHealthCheckTask task = action.new ScheduledHealthCheckTask();
        task.run();

        waitConditionUntilTimeOut(() -> listenerCallCnt.get() == 1, 3000);

        RedisInfoActionContext context = (RedisInfoActionContext)contextRef.get();
        Assert.assertNotNull(context);
        Assert.assertEquals(550515888, context.getResult().getUsedMemory());

    }

    @After
    public void afterInfoStatsActionTest() {
        if(redis != null) {
            try {
                redis.stop();
            } catch (Throwable th) {
                th.printStackTrace();
            }
        }
    }
}