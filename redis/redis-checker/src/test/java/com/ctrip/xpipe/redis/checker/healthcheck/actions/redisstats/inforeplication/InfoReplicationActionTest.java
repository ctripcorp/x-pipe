package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.inforeplication;

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

public class InfoReplicationActionTest extends AbstractCheckerTest {

    private String INFO_REPLICATION_RESPONSE = "# Replication\r\nrole:slave\r\n" +
            "master_host:127.0.0.1\r\nmaster_port:6380\r\nmaster_link_status:up\r\nmaster_last_io_seconds_ago:0\r\n" +
            "master_sync_in_progress:0\r\nslave_repl_offset:3971052969\r\nslave_priority:100\r\nslave_read_only:1\r\n" +
            "connected_slaves:0\r\nmaster_replid:eec907aaf5ad65d58bc4b06d047ad6fc88bb65d1\r\n" +
            "master_replid2:0000000000000000000000000000000000000000\r\nmaster_repl_offset:3971052969\r\n" +
            "second_repl_offset:-1\r\nrepl_backlog_active:1\r\nrepl_backlog_size:34603008\r\n" +
            "repl_backlog_first_byte_offset:3936449962\r\nrepl_backlog_histlen:34603008\r\n";


    private RedisHealthCheckInstance instance;

    private Server redis;

    private InfoReplicationAction action;

    private AtomicInteger redisCallCnt = new AtomicInteger(0);

    private AtomicInteger listenerCallCnt = new AtomicInteger(0);

    AtomicReference<ActionContext> contextRef = new AtomicReference();

    @Before
    public void setupInfoReplicationActionTest() throws Exception {
        redis = startServer(randomPort(), new Function<String, String>() {
            @Override
            public String apply(String s) {
                redisCallCnt.incrementAndGet();

                if (s.trim().toLowerCase().startsWith("info replication")) {
                    return String.format("$%d\r\n%s\r\n", INFO_REPLICATION_RESPONSE.length(), INFO_REPLICATION_RESPONSE);
                } else {
                    return "+OK\r\n";
                }
            }
        });

        instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.ONE_WAY, redis.getPort());
        action = new InfoReplicationAction(scheduled, instance, executors);

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

    @Test
    public void test() throws TimeoutException {
        AbstractHealthCheckAction.ScheduledHealthCheckTask task = action.new ScheduledHealthCheckTask();
        task.run();

        waitConditionUntilTimeOut(() -> listenerCallCnt.get() == 1, 2000);

        InfoReplicationContext context = (InfoReplicationContext)contextRef.get();
        Assert.assertNotNull(context);
        Assert.assertEquals(3971052969L, context.getResult().getSlaveReplOffset());
    }

    @After
    public void afterInfoReplicationActionTest() throws Exception {
        if (null != redis) redis.stop();
    }
}
