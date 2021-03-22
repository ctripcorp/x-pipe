package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.tombstonesize;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class TombstoneSizeCheckActionTest extends AbstractCheckerTest {

    private TombstoneSizeCheckAction action;

    private RedisHealthCheckInstance instance;

    private Server redis;

    private long tombstoneSize = 0;

    @Before
    public void setupExpireSizeCheckActionTest() throws Exception {
        redis = startServer(randomPort(), new Function<String, String>() {
            @Override
            public String apply(String s) {
                if (s.trim().toLowerCase().startsWith("tombstonesize")) {
                    return mockTombstoneSizeResp();
                } else {
                    return "+OK\r\n";
                }
            }
        });

        instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.BI_DIRECTION, redis.getPort());
        action = new TombstoneSizeCheckAction(scheduled, instance, executors);
    }

    @After
    public void afterExpireSizeCheckActionTest() throws Exception {
        if (null != redis) redis.stop();
    }

    @Test
    public void testAction() throws Exception {
        AtomicInteger callCnt = new AtomicInteger(0);
        AtomicReference<ActionContext> contextRef = new AtomicReference();
        tombstoneSize = Math.abs(randomInt());

        action.addListener(new HealthCheckActionListener() {
            @Override
            public void onAction(ActionContext actionContext) {
                callCnt.incrementAndGet();
                contextRef.set(actionContext);
            }

            @Override
            public boolean worksfor(ActionContext t) {
                return true;
            }

            @Override
            public void stopWatch(HealthCheckAction action) {

            }
        });

        AbstractHealthCheckAction.ScheduledHealthCheckTask task = action.new ScheduledHealthCheckTask();
        task.run();

        waitConditionUntilTimeOut(() -> callCnt.get() == 1, 1000);

        TombstoneSizeActionContext context = (TombstoneSizeActionContext) contextRef.get();
        Assert.assertEquals(1, callCnt.get());
        Assert.assertEquals(tombstoneSize, context.getResult().longValue());
    }

    private String mockTombstoneSizeResp() {
        return String.format(":%d\r\n", tombstoneSize);
    }

}
