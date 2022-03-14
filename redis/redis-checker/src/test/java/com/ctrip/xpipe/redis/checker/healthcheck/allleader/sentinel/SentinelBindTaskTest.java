package com.ctrip.xpipe.redis.checker.healthcheck.allleader.sentinel;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

public class SentinelBindTaskTest {

    private String infoSentinel = "sentinel_masters:1\n" +
            "sentinel_tilt:0\n" +
            "sentinel_running_scripts:0\n" +
            "sentinel_scripts_queue_length:0\n" +
            "sentinel_simulate_failure_flags:0\n" +
            "master0:name=cluster+shard+jq,status=ok,address=10.2.27.55:6399,slaves=4,sentinels=5";

    @Test
    public void test() throws Throwable {

        SentinelManager sentinelManager = Mockito.mock(SentinelManager.class);
        CheckerConfig config = Mockito.mock(CheckerConfig.class);
        CheckerConsoleService checkerConsoleService = Mockito.mock(CheckerConsoleService.class);

        DcMeta dcMeta = new DcMeta("jq");
        dcMeta.addSentinel(new SentinelMeta(1L).setClusterType("one_way").setAddress("127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002"));
        dcMeta.addSentinel(new SentinelMeta(2L).setClusterType("single_dc").setAddress("127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005"));
        dcMeta.addSentinel(new SentinelMeta(3L).setClusterType("cross_dc").setAddress("127.0.0.1:5006,127.0.0.1:5007,127.0.0.1:5008"));
        dcMeta.addSentinel(new SentinelMeta(4L).setClusterType("bi_direction").setAddress("127.0.0.1:5009,127.0.0.1:5010,127.0.0.1:5011"));
        dcMeta.addSentinel(new SentinelMeta(4L).setClusterType("cross_dc").setAddress("127.0.0.1:5012,127.0.0.1:5013,127.0.0.1:5014"));

        dcMeta.addCluster(new ClusterMeta("cluster").addShard(new ShardMeta("shard").setSentinelId(1L)));

        DefaultSentinelBindTask task = new DefaultSentinelBindTask(sentinelManager, dcMeta, ClusterType.SINGLE_DC, checkerConsoleService, config);

        when(sentinelManager.infoSentinel(any())).thenReturn(new AbstractCommand<String>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(infoSentinel);
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });

        task.doExecute();

        verify(checkerConsoleService,times(1)).bindShardSentinel(any(),any(),any(),any(),any());
    }


}
