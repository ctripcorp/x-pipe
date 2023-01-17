package com.ctrip.xpipe.redis.checker.healthcheck.actions.gtidgap;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class GtidGapCheckActionTest extends AbstractCheckerTest {

    private GtidGapCheckAction action;

    private RedisHealthCheckInstance instance;

    private Server redis;

    private static final String mockResp = "# Gtid\r\n" +
            "all:4459c71945e04720aecacad7bd5f9cfad54c83ef:1-211639488:211639508-211642565,7af2e3cfe7b665d223b20d9d017699a2dddeac89:1-5321103,5f1949a377b0c543a7edbb4f6ebf085fec45631b:14-7135151\r\n";

    @Before
    public void setup() throws Exception {
        redis = startServer(randomPort(), new Function<String, String>() {
            @Override
            public String apply(String s) {
                if (s.trim().toLowerCase().startsWith("info gtid")) {
                    return String.format("$%d\r\n%s\r\n", mockResp.length(), mockResp);
                } else {
                    return "+OK\r\n";
                }
            }
        });

        instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.ONE_WAY, redis.getPort());
        action = new GtidGapCheckAction(scheduled, instance, executors);
    }

    @After
    public void clean() throws Exception {
        if (null != redis) redis.stop();
    }

    @Test
    public void testAction() throws Exception {
        AtomicInteger callCnt = new AtomicInteger(0);
        AtomicReference<ActionContext> contextRef = new AtomicReference();

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

        GtidGapCheckActionContext context = (GtidGapCheckActionContext) contextRef.get();
        Assert.assertEquals(1, callCnt.get());
        Assert.assertEquals(1, context.getResult().intValue());
    }


}
