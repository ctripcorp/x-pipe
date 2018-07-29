package com.ctrip.xpipe.redis.core.proxy.parser.route;

import com.ctrip.xpipe.redis.core.metaserver.META_SERVER_SERVICE;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Jul 22, 2018
 */
public class RouteOptionParserTest {

    private String CONTENT = "ROUTE PROXYTLS://127.0.0.1:443 PROXYTCP://127.0.0.1:80 TCP://127.0.0.1:8080";

    private RouteOptionParser parser;

    @Before
    public void beforeRouteOptionParserTest() {
        parser = new RouteOptionParser().read(CONTENT);
    }

    @Test
    public void testGetFinalStation() {
        Assert.assertEquals("TCP://127.0.0.1:8080", parser.getFinalStation());

        parser = new RouteOptionParser().read("ROUTE TCP://127.0.0.1:8080");
        Assert.assertEquals("TCP://127.0.0.1:8080", parser.getFinalStation());

        parser = new RouteOptionParser().read("ROUTE");
        Assert.assertEquals("last-stop", parser.getFinalStation());

    }
}