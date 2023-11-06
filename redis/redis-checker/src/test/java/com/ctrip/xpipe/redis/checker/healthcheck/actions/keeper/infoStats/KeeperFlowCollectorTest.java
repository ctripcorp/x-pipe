package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.infoStats;

import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

/**
 * Created by yu
 * 2023/8/29
 */
public class KeeperFlowCollectorTest extends AbstractCheckerTest {

    private KeeperFlowCollector listener;

    private KeeperHealthCheckInstance instance;

    private KeeperInfoStatsActionContext context;

    @Before
    public void before() throws Exception {
        listener = new KeeperFlowCollector();
        int keeperPort = 6380;
        String keeperIp = "127.0.0.1";
        MockitoAnnotations.initMocks(this);
        instance = newRandomKeeperHealthCheckInstance(keeperIp, keeperPort);
        String info = "# Stats\n" +
                "sync_full:0\n" +
                "sync_partial_ok:0\n" +
                "sync_partial_err:0\n" +
                "total_net_input_bytes:1716640145\n" +
                "instantaneous_input_kbps:1.584961\n" +
                "total_net_output_bytes:97539279\n" +
                "instantaneous_output_kbps:0.030273\n" +
                "peak_input_kbps:60315\n" +
                "peak_output_kbps:2\n" +
                "psync_fail_send:0";

//        InfoResultExtractor extractors = new InfoResultExtractor(info);
        context = new KeeperInfoStatsActionContext(instance, info);
    }

    @Test
    public void testParseResult() {
        listener.onAction(context);
        Assert.assertTrue(listener.worksfor(context));
        Assert.assertEquals(1, listener.getHostPort2InputFlow().size());
    }

    @Test
    public void testWithNonResult() {
        String info = "# Stats\n" +
                "sync_full:0\n" +
                "sync_partial_ok:0\n" +
                "sync_partial_err:0\n" +
                "psync_fail_send:0";
        context = new KeeperInfoStatsActionContext(instance, info);
        listener.onAction(context);
        Assert.assertTrue(listener.worksfor(context));
        Assert.assertEquals(0, listener.getHostPort2InputFlow().size());
    }

}