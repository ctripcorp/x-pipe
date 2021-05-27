package com.ctrip.framework.xpipe.redis.proxy;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @Author limingdong
 * @create 2021/4/28
 */
public class RouteOptionParserTest {

    private static String ROUTE = "ROUTE PROXYTCP://127.0.0.1:80,PROXYTCP://127.0.0.2:80 PROXYTLS://127.0.0.0:443 TCP://127.0.0.3:8383";

    private RouteOptionParser routeOptionParser = new RouteOptionParser();

    @Before
    public void setUp() {
        routeOptionParser.read(ROUTE);
    }

    @Test
    public void getNextEndpoints() {
        List<InetSocketAddress> inetSocketAddresses = routeOptionParser.getNextEndpoints();
        Assert.assertEquals(2, inetSocketAddresses.size());
        Assert.assertTrue(inetSocketAddresses.contains(new InetSocketAddress("127.0.0.1", 80)));
        Assert.assertTrue(inetSocketAddresses.contains(new InetSocketAddress("127.0.0.2", 80)));
    }

    @Test
    public void testOutput() {
        String output =  routeOptionParser.output();
        Assert.assertEquals("ROUTE PROXYTLS://127.0.0.0:443 TCP://127.0.0.3:8383", output);
    }
}