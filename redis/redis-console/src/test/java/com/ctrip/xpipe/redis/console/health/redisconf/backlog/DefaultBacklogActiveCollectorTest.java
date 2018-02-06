package com.ctrip.xpipe.redis.console.health.redisconf.backlog;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.health.redisconf.RedisConf;
import com.ctrip.xpipe.redis.console.health.redisconf.RedisConfManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Feb 05, 2018
 */

public class DefaultBacklogActiveCollectorTest {

    @Spy
    private AlertManager alertManager;

    @Mock
    private RedisConfManager redisConfManager;

    @InjectMocks
    private DefaultBacklogActiveCollector collector = new DefaultBacklogActiveCollector();

    @Before
    public void beforeDefaultBacklogActiveCollectorTest() {
        MockitoAnnotations.initMocks(this);

    }

    @Test
    public void analysisInfoReplication() throws Exception {
        doNothing().when(alertManager).alert(any(), any(), any(), any(), any());
        Assert.assertNotNull(redisConfManager);
        RedisConf redisConf = new RedisConf(new HostPort(), "", "");
        redisConf.setXredisVersion("1.0.0");
        redisConf.setRedisVersion("4.0.1");
        when(redisConfManager.findOrCreateConfig("127.0.0.1", 6379)).thenReturn(redisConf);
        collector.analysisInfoReplication("role:slave\n" +
                "master_host:10.2.54.233\n" +
                "master_port:7381\n" +
                "master_link_status:up\n" +
                "master_last_io_seconds_ago:0\n" +
                "master_sync_in_progress:0\n" +
                "slave_repl_offset:1439009182\n" +
                "slave_priority:100\n" +
                "slave_read_only:1\n" +
                "connected_slaves:0\n" +
                "master_replid:204b8d599765dd3dead6faa089aeb77d9d8726f5\n" +
                "master_replid2:0000000000000000000000000000000000000000\n" +
                "master_repl_offset:1439009182\n" +
                "second_repl_offset:-1\n" +
                "repl_backlog_active:0\n" +
                "repl_backlog_size:104857600\n" +
                "repl_backlog_first_byte_offset:1340037212\n" +
                "repl_backlog_histlen:98971971", "cluster", "shard", new HostPort("127.0.0.1", 6379));
        verify(alertManager).alert(any(), any(), any(), any(), any());

    }

}