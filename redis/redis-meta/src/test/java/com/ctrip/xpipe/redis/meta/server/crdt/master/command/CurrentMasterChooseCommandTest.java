package com.ctrip.xpipe.redis.meta.server.crdt.master.command;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@RunWith(MockitoJUnitRunner.class)
public class CurrentMasterChooseCommandTest extends AbstractMetaServerTest {

    private int checkRedisTimeoutSeconds = 1;

    private CurrentMasterChooseCommand chooseCommand;

    private String clusterId = "cluster1", shardId = "shard1";

    private List<RedisMeta> redises = new ArrayList<>();

    private Server redis1, redis2;

    private boolean redis1Master = false, redis2Master = false;

    private boolean supprtGid = true;

    private int gid = 1;

    private String masterResponse = "*3\r\n" +
            "$6\r\n" +
            "master\r\n" +
            ":2606860409\r\n" +
            "*2\r\n" +
            "*3\r\n" +
            "$12\r\n" +
            "10.5.109.154\r\n" +
            "$4\r\n" +
            "6429\r\n" +
            "$10\r\n" +
            "2606860225\r\n" +
            "*3\r\n" +
            "$11\r\n" +
            "10.2.24.215\r\n" +
            "$4\r\n" +
            "6459\r\n" +
            "$10\r\n" +
            "2606860150\r\n";
    private String slaveResponse = "*5\r\n" +
            "$5\r\n" +
            "slave\r\n" +
            "$11\r\n" +
            "10.5.109.50\r\n" +
            ":7379\r\n" +
            "$9\r\n" +
            "connected\r\n" +
            ":2469738246\r\n";

    @Before
    public void setupDefaultPeerMasterChooseCommandTest() throws Exception {
        mockRedises();
        chooseCommand = new CurrentMasterChooseCommand(clusterId, shardId, redises, scheduled,
                getXpipeNettyClientKeyedObjectPool(), checkRedisTimeoutSeconds);
    }

    @After
    public void afterDefaultPeerMasterChooseCommandTest() throws Exception {
        if (null != redis1) redis1.stop();
        if (null != redis2) redis2.stop();
    }

    @Test
    public void testNormalChoose() throws Exception {
        redis1Master = true;
        redis2Master = false;
        RedisMeta redisMeta = chooseCommand.choose();
        Assert.assertNotNull(redisMeta);
        Assert.assertEquals(gid, redisMeta.getGid().intValue());
        Assert.assertEquals("127.0.0.1", redisMeta.getIp());
        Assert.assertEquals(redis1.getPort(), redisMeta.getPort().intValue());
    }

    @Test
    public void testChooseNoMaster() throws Exception {
        redis1Master = false;
        redis2Master = false;
        RedisMeta redisMeta = chooseCommand.choose();
        Assert.assertNull(redisMeta);
    }

    @Test
    public void testChooseMultiMaster() throws Exception {
        redis1Master = true;
        redis2Master = true;
        RedisMeta redisMeta = chooseCommand.choose();
        Assert.assertNull(redisMeta);
    }

    @Test(expected = IllegalStateException.class)
    public void testGetGidFail() throws Exception {
        redis1Master = true;
        redis2Master = false;
        supprtGid = false;
        chooseCommand.choose();
    }

    @Test
    public void testMasterTimeout() throws Exception {
        redis1Master = true;
        redis2Master = false;
        redis1.stop();
        redis1 = null;

        RedisMeta redisMeta = chooseCommand.choose();
        Assert.assertNull(redisMeta);
    }

    @Test
    public void testSlaveTimeout() throws Exception {
        redis1Master = true;
        redis2Master = false;
        redis2.stop();
        redis2 = null;

        RedisMeta redisMeta = chooseCommand.choose();
        Assert.assertNotNull(redisMeta);
        Assert.assertEquals(redis1.getPort(), redisMeta.getPort().intValue());
        Assert.assertEquals(gid, redisMeta.getGid().intValue());
    }

    protected void mockRedises() throws Exception {
        redis1 = startServer(randomPort(), new Function<String, String>() {
            @Override
            public String apply(String s) {
                if (s.startsWith("role")) {
                    return redis1Master ? masterResponse : slaveResponse;
                } else if (s.startsWith("crdt.info")) {
                    return supprtGid ? "$5\r\ngid:" + gid + "\r\n" : "$6\r\ntest:1\r\n";
                }
                return "+OK\r\n";
            }
        });

        redis2 = startServer(randomPort(), new Function<String, String>() {
            @Override
            public String apply(String s) {
                if (s.startsWith("role")) {
                    return redis2Master ? masterResponse : slaveResponse;
                } else if (s.startsWith("crdt.info")) {
                    return supprtGid ? "$5\r\ngid:" + gid + "\r\n" : "$6\r\ntest:1\r\n";
                }
                return "+OK\r\n";
            }
        });

        redises.add(new RedisMeta().setIp("127.0.0.1").setPort(redis1.getPort()));
        redises.add(new RedisMeta().setIp("127.0.0.1").setPort(redis2.getPort()));
    }

}
