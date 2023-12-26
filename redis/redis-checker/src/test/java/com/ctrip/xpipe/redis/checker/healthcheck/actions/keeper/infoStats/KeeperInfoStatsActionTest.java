package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.infoStats;

import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.AbstractHealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.function.Function;

import static org.mockito.Mockito.doNothing;

/**
 * Created by yu
 * 2023/8/28
 */
public class KeeperInfoStatsActionTest extends AbstractCheckerTest {

    private KeeperInfoStatsAction action;

    @Mock
    private KeeperInfoStatsActionListener listener;

    private KeeperInfoStatsActionContext context = null;

    Server redis;

    @Before
    public void beforeVersionCheckActionTest() throws Exception {
        int keeperPort = 6380;
        String keeperIp = "127.0.0.1";
        MockitoAnnotations.initMocks(this);
        KeeperHealthCheckInstance instance = newRandomKeeperHealthCheckInstance(keeperIp, keeperPort);
        action = new KeeperInfoStatsAction(scheduled, instance, executors);
        String info = "# Stats\n" +
                "sync_full:0\n" +
                "sync_partial_ok:0\n" +
                "sync_partial_err:0\n" +
                "total_net_input_bytes:1716640145\n" +
                "total_net_output_bytes:97539279\n" +
                "instantaneous_input_kbps:0.584961\n" +
                "instantaneous_output_kbps:0.030273\n" +
                "peak_input_kbps:60315\n" +
                "peak_output_kbps:2\n" +
                "psync_fail_send:0";

        redis = startServer(keeperPort, new Function<String, String>() {
            @Override
            public String apply(String s) {

                if (s.trim().toLowerCase().startsWith("info stats")) {
                    return  String.format("$%d\r\n%s\r\n", info.length(), info);
                } else {
                    return "+OK\r\n";
                }
            }
        });
        action.addListener(listener);

        Mockito.when(listener.worksfor(Mockito.any())).thenReturn(true);
        doNothing().when(listener).onAction(Mockito.any());
        Mockito.doAnswer(invocation -> {
            context = invocation.getArgument(0, KeeperInfoStatsActionContext.class);
            return null;
        }).when(listener).onAction(Mockito.any());
    }

    @Test
    public void testDoScheduledTask0Positive() throws Exception {
        AbstractHealthCheckAction.ScheduledHealthCheckTask task = action.new ScheduledHealthCheckTask();
        task.run();
        waitConditionUntilTimeOut(() -> null != context, 3000);
        try {
            Assert.assertEquals(0, context.getResult().getKeeperInstantaneousInputKbps().longValue());
        } catch (NullPointerException e) {

        }
    }

    @After
    public void stopRedis() {
        if(redis != null) {
            try {
                redis.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}