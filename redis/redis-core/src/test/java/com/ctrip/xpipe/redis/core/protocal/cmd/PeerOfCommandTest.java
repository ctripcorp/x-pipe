package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.proxy.ProxyEnabledEndpoint;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import org.junit.Assert;
import org.junit.Test;

public class PeerOfCommandTest extends AbstractRedisTest {
    @Test
    public void testPeerOf() throws Exception {
        int port = randomPort();
        PeerOfCommand command = new PeerOfCommand(getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("localhost", port)),
                1,
                new DefaultEndPoint("127.0.0.1", 0),
                scheduled);
        Assert.assertEquals(ByteBufUtils.readToString(command.getRequest()), "peerof 1 127.0.0.1 0\r\n");
    }

    @Test
    public void testPeerOfWithProxy() throws Exception {
        int port = randomPort();
        PeerOfCommand command = new PeerOfCommand(getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("localhost", port)),
                1,
                new ProxyEnabledEndpoint("127.0.0.1", 0, new DefaultProxyConnectProtocolParser().read("PROXY ROUTE PROXYTCP://127.0.0.1:1")),
                scheduled);
        Assert.assertEquals(ByteBufUtils.readToString(command.getRequest()), "peerof 1 127.0.0.1 0 proxy-type XPIPE-PROXY proxy-servers PROXYTCP://127.0.0.1:1\r\n");
        command = new PeerOfCommand(getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("localhost", port)),
                1,
                new ProxyEnabledEndpoint("127.0.0.1", 0, new DefaultProxyConnectProtocolParser().read("PROXY ROUTE PROXYTCP://127.0.0.1:1 PROXYTLS://127.0.0.1:2")),
                scheduled);
        Assert.assertEquals(ByteBufUtils.readToString(command.getRequest()), "peerof 1 127.0.0.1 0 proxy-type XPIPE-PROXY proxy-servers PROXYTCP://127.0.0.1:1 proxy-params \"PROXYTLS://127.0.0.1:2\"\r\n");
        command = new PeerOfCommand(getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("localhost", port)),
                1,
                new ProxyEnabledEndpoint("127.0.0.1", 0, new DefaultProxyConnectProtocolParser().read("PROXY ROUTE PROXYTCP://127.0.0.1:1 PROXYTLS://127.0.0.1:2 PROXYTLS://127.0.0.1:3")),
                scheduled);
        Assert.assertEquals(ByteBufUtils.readToString(command.getRequest()), "peerof 1 127.0.0.1 0 proxy-type XPIPE-PROXY proxy-servers PROXYTCP://127.0.0.1:1 proxy-params \"PROXYTLS://127.0.0.1:2 PROXYTLS://127.0.0.1:3\"\r\n");
    }

}
