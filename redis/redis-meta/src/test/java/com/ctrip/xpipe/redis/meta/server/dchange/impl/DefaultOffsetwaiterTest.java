package com.ctrip.xpipe.redis.meta.server.dchange.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;
import com.ctrip.xpipe.redis.core.redis.RunidGenerator;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.config.UnitTestServerConfig;
import com.ctrip.xpipe.redis.meta.server.dcchange.ExecutionLog;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.DefaultOffsetwaiter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Callable;

/**
 * @author wenchao.meng
 *         <p>
 *         Sep 13, 2017
 */
public class DefaultOffsetwaiterTest extends AbstractMetaServerTest{

    private DefaultOffsetwaiter offsetwaiter;
    private int waitforOffsetMilli = 400;

    private ExecutionLog executionLog;

    private String host;
    private int port;


    @Before
    public void beforeDefaultOffsetwaiterTest() throws Exception {
        offsetwaiter = new DefaultOffsetwaiter();
        offsetwaiter.setKeyedObjectPool(getXpipeNettyClientKeyedObjectPool());
        offsetwaiter.setMetaServerConfig(new UnitTestServerConfig().setWaitforOffsetMilli(waitforOffsetMilli));
        offsetwaiter.setScheduled(scheduled);

        executionLog = new ExecutionLog(currentTestName());

        host = "127.0.0.1";
        port = randomPort();
    }

    @Test
    public void testWaitforMaster() throws Exception {

        MasterInfo masterInfo = new MasterInfo(RunidGenerator.DEFAULT.generateRunid(), 1L);
        startServer(port, toRedisProtocalString("# Replication\r\n" +
                "role:master\r\n" +
                "connected_slaves:1\r\n" +
                "slave0:ip=127.0.0.1,port=6479,state=online,offset=12936,lag=0\r\n" +
                "master_replid:ebe8dc36b4a9901af79ac117fa55d45e3dbb92a1\r\n" +
                "master_replid2:0000000000000000000000000000000000000000\r\n" +
                "master_repl_offset:12936\r\n" +
                "second_repl_offset:-1\r\n" +
                "repl_backlog_active:1\r\n" +
                "repl_backlog_size:1048576\r\n" +
                "repl_backlog_first_byte_offset:1\r\n" +
                "repl_backlog_histlen:12936"));
        Assert.assertFalse(offsetwaiter.tryWaitfor(new HostPort(host, port), masterInfo, executionLog));
    }

    @Test
    public void testWaitforSlaveDifferentReplid() throws Exception {

        MasterInfo masterInfo = new MasterInfo(RunidGenerator.DEFAULT.generateRunid(), 1L);
        startServer(port, toRedisProtocalString("# Replication\r\n" +
                "role:slave\r\n" +
                "master_host:127.0.0.1\r\n" +
                "master_port:6379\r\n" +
                "master_link_status:up\r\n" +
                "master_last_io_seconds_ago:8\r\n" +
                "master_sync_in_progress:0\r\n" +
                "slave_repl_offset:13202\r\n" +
                "slave_priority:100\r\n" +
                "slave_read_only:1\r\n" +
                "connected_slaves:0\r\n" +
                "master_replid:ebe8dc36b4a9901af79ac117fa55d45e3dbb92a1\r\n" +
                "master_replid2:0000000000000000000000000000000000000000\r\n" +
                "master_repl_offset:13202\r\n" +
                "second_repl_offset:-1\r\n" +
                "repl_backlog_active:1\r\n" +
                "repl_backlog_size:1048576\r\n" +
                "repl_backlog_first_byte_offset:1\r\n" +
                "repl_backlog_histlen:13202"));
        Assert.assertFalse(offsetwaiter.tryWaitfor(new HostPort(host, port), masterInfo, executionLog));

    }

    @Test
    public void testWaitTimeout() throws Exception {

        String masterReplId = RunidGenerator.DEFAULT.generateRunid();
        Long masterOffset = 10000L;

        MasterInfo masterInfo = new MasterInfo(masterReplId, masterOffset);

        startServer(port, new Callable<String>() {

            private Long offset = masterOffset - 200;
            @Override
            public String call() throws Exception {
                return toRedisProtocalString("# Replication\r\n" +
                        "role:slave\r\n" +
                        "master_host:127.0.0.1\r\n" +
                        "master_port:6379\r\n" +
                        "master_link_status:up\r\n" +
                        "master_last_io_seconds_ago:8\r\n" +
                        "master_sync_in_progress:0\r\n" +
                        "slave_repl_offset:" + offset + "\r\n" +
                        "slave_priority:100\r\n" +
                        "slave_read_only:1\r\n" +
                        "connected_slaves:0\r\n" +
                        "master_replid:" + masterReplId + "\r\n" +
                        "master_replid2:0000000000000000000000000000000000000000\r\n" +
                        "master_repl_offset:" +offset+"\r\n" +
                        "second_repl_offset:-1\r\n" +
                        "repl_backlog_active:1\r\n" +
                        "repl_backlog_size:1048576\r\n" +
                        "repl_backlog_first_byte_offset:1\r\n" +
                        "repl_backlog_histlen:13202");
            }
        });
        Assert.assertFalse(offsetwaiter.tryWaitfor(new HostPort(host, port), masterInfo, executionLog));
    }

    @Test
    public void testWaitAndSucceed() throws Exception {

        String masterReplId = RunidGenerator.DEFAULT.generateRunid();
        Long masterOffset = 10000L;

        MasterInfo masterInfo = new MasterInfo(masterReplId, masterOffset);

        startServer(port, new Callable<String>() {
            private Long offset = masterOffset - 200;
            @Override
            public String call() throws Exception {
                offset += 100;
                return toRedisProtocalString("# Replication\r\n" +
                        "role:slave\r\n" +
                        "master_host:127.0.0.1\r\n" +
                        "master_port:6379\r\n" +
                        "master_link_status:up\r\n" +
                        "master_last_io_seconds_ago:8\r\n" +
                        "master_sync_in_progress:0\r\n" +
                        "slave_repl_offset:" + offset + "\r\n" +
                        "slave_priority:100\r\n" +
                        "slave_read_only:1\r\n" +
                        "connected_slaves:0\r\n" +
                        "master_replid:" + masterReplId + "\r\n" +
                        "master_replid2:0000000000000000000000000000000000000000\r\n" +
                        "master_repl_offset:" +offset+"\r\n" +
                        "second_repl_offset:-1\r\n" +
                        "repl_backlog_active:1\r\n" +
                        "repl_backlog_size:1048576\r\n" +
                        "repl_backlog_first_byte_offset:1\r\n" +
                        "repl_backlog_histlen:13202");
            }
        });
        Assert.assertTrue(offsetwaiter.tryWaitfor(new HostPort(host, port), masterInfo, executionLog));
    }

    @Test
    public void testWaitAndSucceed2_8_19() throws Exception {

        String masterReplId = RunidGenerator.DEFAULT.generateRunid();
        Long masterOffset = 10000L;

        MasterInfo masterInfo = new MasterInfo(masterReplId, masterOffset);

        startServer(port, new Callable<String>() {
            private Long offset = masterOffset - 200;
            @Override
            public String call() throws Exception {
                offset += 100;
                return toRedisProtocalString("# Replication\r\n" +
                        "role:slave\r\n" +
                        "master_host:127.0.0.1\r\n" +
                        "master_port:6379\r\n" +
                        "master_link_status:up\r\n" +
                        "master_last_io_seconds_ago:1\r\n" +
                        "master_sync_in_progress:0\r\n" +
                        "slave_repl_offset:"+offset+"\r\n" +
                        "slave_priority:100\r\n" +
                        "slave_read_only:1\r\n" +
                        "connected_slaves:0\r\n" +
                        "master_repl_offset:0\r\n" +
                        "repl_backlog_active:0\r\n" +
                        "repl_backlog_size:104857600\r\n" +
                        "repl_backlog_first_byte_offset:0\r\n" +
                        "repl_backlog_histlen:0");
            }
        });

        Assert.assertTrue(offsetwaiter.tryWaitfor(new HostPort(host, port), masterInfo, executionLog));
    }

}
