package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.infostats;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class InfoStatsActionTest extends AbstractCheckerTest {

    private RedisHealthCheckInstance instance;

    private Server redis;

    private InfoStatsAction action;

    private AtomicInteger redisCallCnt = new AtomicInteger(0);

    private AtomicInteger listenerCallCnt = new AtomicInteger(0);

    AtomicReference<ActionContext> contextRef = new AtomicReference();

    private static final String INFO_STATS_RESPONSE = "# Stats\r\nsync_full:99\r\nsync_partial_ok:24\r\nsync_partial_err:97\r\n";

    @Before
    public void beforeInfoStatsActionTest() throws Exception {
        redis = startServer(randomPort(), new Function<String, String>() {
            @Override
            public String apply(String s) {
                redisCallCnt.incrementAndGet();

                if (s.trim().toLowerCase().startsWith("info stats")) {
                    return  String.format("$%d\r\n%s\r\n", INFO_STATS_RESPONSE.length(), INFO_STATS_RESPONSE);
                } else {
                    return "+OK\r\n";
                }
            }
        });

        instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.BI_DIRECTION, redis.getPort());
        action = new InfoStatsAction(scheduled, instance, executors);

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

        InfoStatsContext context = (InfoStatsContext)contextRef.get();
        Assert.assertNotNull(context);
        Assert.assertEquals(99L, context.getResult().getSyncFull());
        Assert.assertEquals(24L, context.getResult().getSyncPartialOk());
        Assert.assertEquals(97L, context.getResult().getSyncPartialErr());
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
