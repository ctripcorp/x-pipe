package com.ctrip.xpipe.redis.core.proxy.protocols;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.CompressAlgorithm;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.proxy.ProxyEnabledEndpoint;
import com.ctrip.xpipe.redis.core.protocal.cmd.PingCommand;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.endpoint.NaiveNextHopAlgorithm;
import com.ctrip.xpipe.redis.core.proxy.netty.ProxyEnabledNettyKeyedPoolClientFactory;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.resource.ConsoleProxyResourceManager;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;

public class DefaultProxyConnectProtocolTest extends AbstractTest {

    private DefaultProxyConnectProtocol protocol;

    private DefaultProxyConnectProtocolParser parser = new DefaultProxyConnectProtocolParser();

    private String proto;

    @Before
    public void beforeDefaultProxyConnectProtocolTest() {
        proto = "PROXY ROUTE PROXYTLS://10.5.111.148:443 TCP://10.2.24.215:6390#;FORWARD_FOR 10.5.109.209:41085;";
        protocol = (DefaultProxyConnectProtocol) parser.read(proto);
    }

    @Test
    public void nextEndpoints() {
        Assert.assertEquals(Lists.newArrayList(new DefaultProxyEndpoint("PROXYTLS://10.5.111.148:443")),
                protocol.nextEndpoints());

    }

    @Test
    public void recordForwardFor() {
        parser = new DefaultProxyConnectProtocolParser();
        protocol = (DefaultProxyConnectProtocol) parser.read("PROXY ROUTE TCP://127.0.0.1:6379#;");
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", randomPort());
        protocol.recordForwardFor(address);
        logger.info("{}", protocol.getForwardFor());
        Assert.assertEquals(String.format("FORWARD_FOR %s:%d",
                address.getAddress().getHostAddress(), address.getPort()), protocol.getForwardFor());
    }

    @Test
    public void getForwardFor() {
        protocol = (DefaultProxyConnectProtocol) parser.read("PROXY ROUTE TCP://127.0.0.1:6379#;");
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", randomPort());
        protocol.recordForwardFor(address);
        logger.info("{}", protocol.getForwardFor());
    }

    @Test
    public void setContent() {
        String newContent = "PROXY ROUTE PROXYTLS://10.5.111.148:443 TCP://10.2.24.215:6390;null;";
        protocol.setContent(newContent);
        Assert.assertEquals(newContent, protocol.getContent());
    }

    @Test
    public void testGetContent() {
        Assert.assertEquals(proto, protocol.getContent());
    }

    @Test
    public void testGetRouteInfo() {
        Assert.assertEquals("PROXYTLS://10.5.111.148:443 TCP://10.2.24.215:6390", protocol.getRouteInfo());
    }

    @Test
    public void testGetFinalStation() {
        Assert.assertEquals("TCP://10.2.24.215:6390", protocol.getFinalStation());
    }

    @Test
    public void testGetSource() {
        Assert.assertEquals("10.5.109.209:41085", protocol.getSource());
    }

    @Test
    public void testIsCompressed() {
        Assert.assertFalse(protocol.isCompressed());
        parser = new DefaultProxyConnectProtocolParser();
        protocol = (DefaultProxyConnectProtocol) parser.read("PROXY ROUTE PROXYTLS://10.5.111.148:443 TCP://10.2.24.215:6390#;FORWARD_FOR 10.5.109.209:41085;CONTENT COMPRESS ZSTD 1.0#;");
        Assert.assertTrue(protocol.isCompressed());
    }

    @Test
    public void testGetCompressAlgorithm() {
        Assert.assertFalse(protocol.isCompressed());
        DefaultProxyConnectProtocolParser parser = new DefaultProxyConnectProtocolParser();
        DefaultProxyConnectProtocol protocol = (DefaultProxyConnectProtocol) parser.read("PROXY ROUTE PROXYTLS://10.5.111.148:443 TCP://10.2.24.215:6390#;FORWARD_FOR 10.5.109.209:41085;CONTENT COMPRESS ZSTD 1.0#;");
        Assert.assertTrue(protocol.isCompressed());
        Assert.assertEquals(CompressAlgorithm.AlgorithmType.ZSTD, protocol.getCompressAlgorithm().getType());
        Assert.assertEquals("1.0", protocol.getCompressAlgorithm().version());
    }

    @Test
    public void testRemoveCompressOptionIfExist() {
        DefaultProxyConnectProtocolParser parser = new DefaultProxyConnectProtocolParser();
        DefaultProxyConnectProtocol protocol = (DefaultProxyConnectProtocol) parser.read("PROXY ROUTE PROXYTLS://10.5.111.148:443 TCP://10.2.24.215:6390#;FORWARD_FOR 10.5.109.209:41085;CONTENT COMPRESS ZSTD 1.0#;");
        Assert.assertTrue(protocol.isCompressed());
        protocol.removeCompressOptionIfExist();
        Assert.assertFalse(protocol.isCompressed());
    }

    @Test
    public void testAddCompression() {
        Assert.assertFalse(protocol.isCompressed());
        protocol.addCompression(new CompressAlgorithm() {
            @Override
            public String version() {
                return "3.0";
            }

            @Override
            public AlgorithmType getType() {
                return AlgorithmType.ZSTD;
            }
        });
        Assert.assertTrue(protocol.isCompressed());
        Assert.assertEquals("3.0", protocol.getCompressAlgorithm().version());
        Assert.assertEquals(CompressAlgorithm.AlgorithmType.ZSTD, protocol.getCompressAlgorithm().getType());
    }

    @Test
    public void testProtocol() {
        String newContent = "PROXY ROUTE TCP://10.2.24.215:6390 PROXYTCP://10.5.111.111:80 PROXYTLS://10.5.111.148:443;null;";
        protocol.setContent(newContent);
        protocol.getParser();
    }


    @Test
    public void endpoint() {
        ProxyResourceManager resourceManager = new ConsoleProxyResourceManager(new NaiveNextHopAlgorithm());
        ProxyEnabledNettyKeyedPoolClientFactory factory = new ProxyEnabledNettyKeyedPoolClientFactory(resourceManager);
        XpipeNettyClientKeyedObjectPool pool = new XpipeNettyClientKeyedObjectPool(factory);
        try {
            pool.initialize();
            pool.start();
        }catch (Exception e) {
            logger.error(e.getMessage());
            return;
        }
        DefaultProxyConnectProtocolParser parser = new DefaultProxyConnectProtocolParser();
        ProxyConnectProtocol protocol = parser.read("PROXY ROUTE PROXYTCP://127.0.0.1:10080 PROXYTLS://127.0.0.0:10444");
        Endpoint point = new ProxyEnabledEndpoint("127.0.0.1", 16379, protocol);
        PingCommand ping_command = new PingCommand(pool.getKeyPool(point), scheduled, 500);
        try {
            ping_command.execute().get();
        }catch (Exception e) {
            logger.error(e.getMessage());
            return;
        }

        point = new DefaultEndPoint("127.0.0.1", 16379);
        ping_command = new PingCommand(pool.getKeyPool(point), scheduled, 500);
        try {
            ping_command.execute().get();
        }catch (Exception e) {
            logger.error(e.getMessage());
            return;
        }
    }

}