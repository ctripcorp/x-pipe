package com.ctrip.xpipe.redis.checker.resource;

import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

/**
 * @author lishanglin
 * date 2021/3/18
 */
public class DefaultCheckerConsoleServiceTest extends AbstractCheckerTest {

    private DefaultCheckerConsoleService service;

    private MockWebServer webServer;

    private String console;

    @Before
    public void setupDefaultCheckerConsoleServiceTest() throws Exception {
        service = new DefaultCheckerConsoleService();
        webServer = new MockWebServer();
        webServer.start(InetAddress.getByName("127.0.0.1"), randomPort());
        console = "http://127.0.0.1:" + webServer.getPort();
    }

    @Test
    public void testGetXpipeMeta() throws Exception {
        webServer.enqueue(new MockResponse().setBody(getXpipeMeta().toString()).setHeader("Content-Type", "application/json"));
        XpipeMeta xpipeMeta = service.getXpipeMeta(console, 1);
        Assert.assertNotNull(xpipeMeta.getDcs().get("jq").getClusters().get("cluster1").parent());
        Assert.assertEquals(1, webServer.getRequestCount());


        Assert.assertEquals(3, xpipeMeta.getRedisCheckRules().values().size());

        RecordedRequest request = webServer.takeRequest();
        Assert.assertEquals("/api/meta/divide/1?format=xml", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }

    @Override
    protected String getXpipeMetaConfigFile() {
        return "dc-meta-test.xml";
    }

}
