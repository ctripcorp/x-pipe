package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.RedisProxyMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.impl.XpipeRedisProxy;
import org.junit.Assert;
import org.junit.Test;
import io.netty.buffer.ByteBuf;

import java.util.List;

public class PeerOfCommandTest extends AbstractRedisTest {



    @Test
    public void testPeerOf2Buf1() throws Exception {
        int port = randomPort();
        PeerOfCommand command = new PeerOfCommand(getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("localhost", port))
                ,1,
                "127.0.0.1",
                0,
                scheduled);

        Assert.assertEquals(ByteBufUtils.readToString(command.getRequest()), "peerof 1 127.0.0.1 0\r\n");

    }

    @Test
    public void testPeerOf2Buf2() throws Exception {

        XpipeRedisProxy proxy = XpipeRedisProxy.read("PROXYTCP://127.0.0.1:1");
        int port = randomPort();
        PeerOfCommand command = new PeerOfCommand(getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("localhost", port))
                ,1,
                "127.0.0.1",
                0,
                proxy,
                scheduled);



        Assert.assertEquals(ByteBufUtils.readToString(command.getRequest()), "peerof 1 127.0.0.1 0 proxy-type XPIPE-PROXY proxy-server 127.0.0.1:1\r\n");

    }

    @Test
    public void testPeerOf2Buf3() throws Exception {
        XpipeRedisProxy proxy = XpipeRedisProxy.read("PROXYTCP://127.0.0.1:1 PROXYTLS://127.0.0.1:10");
        int port = randomPort();
        PeerOfCommand command = new PeerOfCommand(getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("localhost", port
                ))
                ,1,
                "127.0.0.1",
                0,
                proxy,
                scheduled);


        Assert.assertEquals(ByteBufUtils.readToString(command.getRequest()), "peerof 1 127.0.0.1 0 proxy-type XPIPE-PROXY proxy-server 127.0.0.1:1 proxy-params PROXYTLS://127.0.0.1:10\r\n");
    }

    @Test
    public void testPeerOf2Buf4() throws Exception {
        XpipeRedisProxy proxy = XpipeRedisProxy.read("PROXYTCP://127.0.0.1:1 PROXYTLS://127.0.0.1:10,PROXYTLS://127.0.0.1:20");
        int port = randomPort();
        PeerOfCommand command = new PeerOfCommand(getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("localhost", port
        ))
                ,1,
                "127.0.0.1",
                0,
                proxy,
                scheduled);


        Assert.assertEquals(ByteBufUtils.readToString(command.getRequest()), "peerof 1 127.0.0.1 0 proxy-type XPIPE-PROXY proxy-server 127.0.0.1:1 proxy-params PROXYTLS://127.0.0.1:10,PROXYTLS://127.0.0.1:20\r\n");
    }


}
