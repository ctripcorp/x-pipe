package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.tps;

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

public class TpsCheckActionTest extends AbstractCheckerTest {

    private Server redis;

    private int redisTps = 0;

    private RedisHealthCheckInstance instance;

    private TpsCheckAction action;

    private AtomicInteger callCnt = new AtomicInteger(0);

    private AtomicReference<ActionContext> contextRef = new AtomicReference();

    @Before
    public void setupTpsCheckActionTest() throws Exception {
        redis = startServer(randomPort(), new Function<String, String>() {
            @Override
            public String apply(String s) {
                if (s.trim().toLowerCase().startsWith("info stats")) {
                    return mockInfoResp();
                } else {
                    return "+OK\r\n";
                }
            }
        });

        instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.BI_DIRECTION, redis.getPort());
        action = new TpsCheckAction(scheduled, instance, executors);
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
    }

    @After
    public void afterTpsCheckActionTest() throws Exception {
        if (null != redis) redis.stop();
    }

    private String mockInfoResp() {
        String content = "instantaneous_ops_per_sec:" + redisTps + "\r\n";
        return String.format("$%d\r\n%s\r\n", content.length(), content);
    }

    @Test
    public void testDoAction() throws Exception {
        redisTps = Math.abs(randomInt());
        AbstractHealthCheckAction.ScheduledHealthCheckTask task = action.new ScheduledHealthCheckTask();
        task.run();

        waitConditionUntilTimeOut(() -> callCnt.get() == 1, 1000);

        TpsActionContext context = (TpsActionContext) contextRef.get();
        Assert.assertEquals(1, callCnt.get());
        Assert.assertEquals(redisTps, context.getResult().longValue());
    }

}
