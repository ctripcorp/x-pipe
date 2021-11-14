package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.conflic;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class ConflictCheckActionTest extends AbstractCheckerTest {

    private RedisHealthCheckInstance instance;

    private Server redis;

    ConflictCheckAction action;

    private volatile int redisDelay = 0;

    private AtomicInteger redisCallCnt = new AtomicInteger(0);

    private AtomicInteger listenerCallCnt = new AtomicInteger(0);

    AtomicReference<ActionContext> contextRef = new AtomicReference();

    private long typeConflict = 0L;
    private long nonTypeConflict = 0L;
    private long modifyConflict = 0L;
    private long mergeConflict = 0L;
    private long setConflict = 0L;
    private long delConflict = 0L;
    private long setDelConflict = 0L;

    private boolean lowVersion = true;

    private String TEMP_OLD_STATS_RESP = "crdt_type_conflict:%d\r\n" +
            "crdt_non_type_conflict:%d\r\n" +
            "crdt_modify_conflict:%d\r\n" +
            "crdt_merge_conflict:%d\r\n";

    private String TEMP_STATS_RESP ="crdt_conflict:type=%d,set=%d,del=%d,set_del=%d\r\n" +
            "crdt_conflict_op:modify=%d,merge=%d\r\n";

    @Before
    public void setupConflictCheckActionTest() throws Exception {
        redis = startServer(randomPort(), new Function<String, String>() {
            @Override
            public String apply(String s) {
                redisCallCnt.incrementAndGet();
                if (redisDelay > 0) sleep(redisDelay);

                if (s.trim().toLowerCase().startsWith("crdt.info stats")) {
                    return mockConflictResponse();
                } else {
                    return "+OK\r\n";
                }
            }
        });

        instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.BI_DIRECTION, redis.getPort());
        action = new ConflictCheckAction(scheduled, instance, executors);

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

    @Test
    public void testDoTaskWithLowVersion() throws Exception {
        typeConflict = Math.abs(randomInt());
        nonTypeConflict = Math.abs(randomInt());
        mergeConflict = Math.abs(randomInt());
        modifyConflict = Math.abs(randomInt());

        AbstractHealthCheckAction.ScheduledHealthCheckTask task = action.new ScheduledHealthCheckTask();
        task.run();

        waitConditionUntilTimeOut(() -> listenerCallCnt.get() == 1, 1000);

        CrdtConflictCheckContext context = (CrdtConflictCheckContext) contextRef.get();
        Assert.assertEquals(1, listenerCallCnt.get());
        Assert.assertEquals(new Long(typeConflict), context.getResult().getTypeConflict());
        Assert.assertEquals(new Long(nonTypeConflict), context.getResult().getNonTypeConflict());
        Assert.assertEquals(new Long(mergeConflict), context.getResult().getMergeConflict());
        Assert.assertEquals(new Long(modifyConflict), context.getResult().getModifyConflict());
        Assert.assertNull(context.getResult().getSetConflict());
        Assert.assertNull(context.getResult().getDelConflict());
        Assert.assertNull(context.getResult().getSetDelConflict());
    }

    @Test
    public void testDoTask() throws Exception {
        typeConflict = Math.abs(randomInt());
        setConflict = Math.abs(randomInt());
        delConflict = Math.abs(randomInt());
        setDelConflict = Math.abs(randomInt());
        mergeConflict = Math.abs(randomInt());
        modifyConflict = Math.abs(randomInt());
        lowVersion = false;

        AbstractHealthCheckAction.ScheduledHealthCheckTask task = action.new ScheduledHealthCheckTask();
        task.run();

        waitConditionUntilTimeOut(() -> listenerCallCnt.get() == 1, 1000);

        CrdtConflictCheckContext context = (CrdtConflictCheckContext) contextRef.get();
        Assert.assertEquals(1, listenerCallCnt.get());
        Assert.assertEquals(new Long(typeConflict), context.getResult().getTypeConflict());
        Assert.assertEquals(new Long(setConflict), context.getResult().getSetConflict());
        Assert.assertEquals(new Long(delConflict), context.getResult().getDelConflict());
        Assert.assertEquals(new Long(setDelConflict), context.getResult().getSetDelConflict());
        Assert.assertEquals(new Long(setConflict + delConflict + setDelConflict), context.getResult().getNonTypeConflict());
        Assert.assertEquals(new Long(mergeConflict), context.getResult().getMergeConflict());
        Assert.assertEquals(new Long(modifyConflict), context.getResult().getModifyConflict());
    }

    @Test
    public void testRedisHang() {
        redisDelay = AbstractRedisCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI + 10;
        AbstractHealthCheckAction.ScheduledHealthCheckTask task = action.new ScheduledHealthCheckTask();
        task.run();
        sleep(redisDelay + 200);
        Assert.assertEquals(0, listenerCallCnt.get());
    }

    @Test
    public void testDoActionTooQuickly() {
        redisDelay = AbstractRedisCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI / 2;
        AbstractHealthCheckAction.ScheduledHealthCheckTask task = action.new ScheduledHealthCheckTask();
        task.run();
        sleep(1);
        task.run();

        sleep(redisDelay);
        Assert.assertEquals(1, redisCallCnt.get());
    }

    private String mockConflictResponse() {
        String content;
        if (lowVersion) {
            content = String.format(TEMP_OLD_STATS_RESP, typeConflict, nonTypeConflict, modifyConflict, mergeConflict);
        } else {
            content = String.format(TEMP_STATS_RESP, typeConflict, setConflict, delConflict, setDelConflict, modifyConflict, mergeConflict);
        }
        return String.format("$%d\r\n%s\r\n", content.length(), content);
    }



}
