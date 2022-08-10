package com.ctrip.xpipe.redis.core.proxy.protocols;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.proxy.CompressAlgorithm;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.parser.route.RouteOptionParser;
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
    public void testIsNearDest() {
        DefaultProxyConnectProtocolParser parser1 = new DefaultProxyConnectProtocolParser();
        DefaultProxyConnectProtocol protocol1 = (DefaultProxyConnectProtocol) parser1.read("PROXY ROUTE TCP://127.0.0.1:6379;FORWARD_FOR 10.128.12.20:45700;CONTENT COMPRESS ZSTD 1.0#");
        Assert.assertEquals(true, protocol1.isNearDest());
        RouteOptionParser routeParser = (RouteOptionParser) parser1.getProxyOptionParser(PROXY_OPTION.ROUTE);
        Assert.assertNotNull(routeParser);
        parser1.removeProxyOptionParser(parser1.getProxyOptionParser(PROXY_OPTION.ROUTE));


        protocol1 = (DefaultProxyConnectProtocol) parser1.read("PROXY ROUTE PROXYTLS://127.0.0.1:443,PROXYTLS://127.0.0.2:443 TCP://127.0.0.1:6379");
        Assert.assertEquals(false, protocol1.isNearDest());
        routeParser = (RouteOptionParser) parser1.getProxyOptionParser(PROXY_OPTION.ROUTE);
        Assert.assertNotNull(routeParser);
        parser1.removeProxyOptionParser(routeParser);
        Assert.assertNull(parser1.getProxyOptionParser(PROXY_OPTION.ROUTE));

        protocol1 = (DefaultProxyConnectProtocol) parser1.read("PROXY ROUTE PROXYTCP://127.0.0.1:80 PROXYTLS://127.0.0.1:443 TCP://127.0.0.1:6379");
        Assert.assertEquals(false, protocol1.isNearDest());
        routeParser = (RouteOptionParser) parser1.getProxyOptionParser(PROXY_OPTION.ROUTE);
        Assert.assertNotNull(routeParser);
        parser1.removeProxyOptionParser(routeParser);
        Assert.assertNull(parser1.getProxyOptionParser(PROXY_OPTION.ROUTE));

        protocol1 = (DefaultProxyConnectProtocol) parser1.read("PROXY ROUTE ");
        Assert.assertEquals(true, protocol1.isNearDest());
        routeParser = (RouteOptionParser) parser1.getProxyOptionParser(PROXY_OPTION.ROUTE);
        Assert.assertNotNull(routeParser);
        parser1.removeProxyOptionParser(routeParser);
        Assert.assertNull(parser1.getProxyOptionParser(PROXY_OPTION.ROUTE));
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
}