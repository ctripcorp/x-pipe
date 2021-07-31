package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.backstreaming;

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

/**
 * @author lishanglin
 * date 2021/1/26
 */
public class BackStreamingActionTest extends AbstractCheckerTest {

    private static final String LOW_VERSION_REPLICATION = "# CRDT Replication\r\novc:1:0;2:0\r\ngcvc:1:0;2:0\r\ngid:1\r\n";
    private static final String TMP_HIGH_VERSION_REPLICATION = "# CRDT Replication\r\n" +
            "ovc:1:0;2:0\r\n" +
            "gcvc:1:0;2:0\r\n" +
            "gid:1\r\n" +
            "backstreaming:%s\r\n";

    private RedisHealthCheckInstance instance;

    private Server redis;

    private BackStreamingAction action;

    private boolean lowVersion = false;

    private boolean onBackStreaming = false;

    private AtomicInteger redisCallCnt = new AtomicInteger(0);

    private AtomicInteger listenerCallCnt = new AtomicInteger(0);

    AtomicReference<ActionContext> contextRef = new AtomicReference();

    @Before
    public void setupConflictCheckActionTest() throws Exception {
        redis = startServer(randomPort(), new Function<String, String>() {
            @Override
            public String apply(String s) {
                redisCallCnt.incrementAndGet();

                if (s.trim().toLowerCase().startsWith("crdt.info replication")) {
                    return mockReplicationResponse();
                } else {
                    return "+OK\r\n";
                }
            }
        });

        instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.BI_DIRECTION, redis.getPort());
        action = new BackStreamingAction(scheduled, instance, executors);

        action.addListener(new HealthCheckActionListener() {
            @Override
            public void onAction(ActionContext actionContext) {
                listenerCallCnt.incrementAndGet();
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
    public void afterConflictCheckActionTest() throws Exception {
        if (null != redis) redis.stop();
    }

    private String mockReplicationResponse() {
        String content;
        if (lowVersion) content = LOW_VERSION_REPLICATION;
        else {
            content = String.format(TMP_HIGH_VERSION_REPLICATION, onBackStreaming ? "1" : "0");
        }

        return String.format("$%d\r\n%s\r\n", content.length(), content);
    }

    @Test
    public void testOnBackStream() throws Exception {
        onBackStreaming = true;

        AbstractHealthCheckAction.ScheduledHealthCheckTask task = action.new ScheduledHealthCheckTask();
        task.run();

        waitConditionUntilTimeOut(() -> listenerCallCnt.get() == 1, 1000);

        BackStreamingContext context = (BackStreamingContext) contextRef.get();
        Assert.assertNotNull(context);
        Assert.assertTrue(context.getResult());
    }

    @Test
    public void testNoBackStream() throws Exception {
        onBackStreaming = false;

        AbstractHealthCheckAction.ScheduledHealthCheckTask task = action.new ScheduledHealthCheckTask();
        task.run();

        waitConditionUntilTimeOut(() -> listenerCallCnt.get() == 1, 1000);

        BackStreamingContext context = (BackStreamingContext) contextRef.get();
        Assert.assertNotNull(context);
        Assert.assertFalse(context.getResult());
    }

    @Test
    public void testLowVersion() throws Exception {
        lowVersion = true;

        AbstractHealthCheckAction.ScheduledHealthCheckTask task = action.new ScheduledHealthCheckTask();
        task.run();

        waitConditionUntilTimeOut(() -> listenerCallCnt.get() == 1, 1000);

        BackStreamingContext context = (BackStreamingContext) contextRef.get();
        Assert.assertNotNull(context);
        Assert.assertFalse(context.getResult());
    }

}
