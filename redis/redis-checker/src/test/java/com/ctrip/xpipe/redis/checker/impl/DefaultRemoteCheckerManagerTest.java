package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE.INSTANCEUP;

/**
 * @author lishanglin
 * date 2021/3/17
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultRemoteCheckerManagerTest extends AbstractCheckerTest {

    private DefaultRemoteCheckerManager manager;

    @Mock
    private CheckerConfig checkerConfig;

    private MockWebServer webServer;

    @Before
    public void setupDefaultRemoteCheckerManagerTest() throws Exception {
        this.manager = new DefaultRemoteCheckerManager(checkerConfig);
        webServer = new MockWebServer();
        webServer.start(InetAddress.getByName("127.0.0.1"), randomPort());
        Mockito.when(checkerConfig.getAllCheckerAddress()).thenReturn(Collections.singleton("127.0.0.1:" + webServer.getPort()));
    }

    @Test
    public void testAllHealthStatus() throws Exception {
        webServer.enqueue(new MockResponse().setBody("\"" + INSTANCEUP.name() + "\"").setHeader("Content-Type", "application/json"));
        List<HEALTH_STATE> states = this.manager.allHealthStatus("10.0.0.1", 6379);
        Assert.assertEquals(Collections.singletonList(INSTANCEUP), states);

        Assert.assertEquals(1, webServer.getRequestCount());
        RecordedRequest request = webServer.takeRequest();
        Assert.assertEquals("/api/health/10.0.0.1/6379", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }

}
