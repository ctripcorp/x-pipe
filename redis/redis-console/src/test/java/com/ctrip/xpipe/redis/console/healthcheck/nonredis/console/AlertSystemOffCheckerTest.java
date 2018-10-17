package com.ctrip.xpipe.redis.console.healthcheck.nonredis.console;

import com.ctrip.xpipe.api.cluster.CrossDcClusterServer;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Dec 01, 2017
 */
public class AlertSystemOffCheckerTest extends AbstractConsoleIntegrationTest {

    @Autowired
    @InjectMocks
    AlertSystemOffChecker checker;

    @Mock
    ConfigService configService;

    @Mock
    CrossDcClusterServer clusterServer;

    @Before
    public void beforeAlertSystemOffCheckerTest() {
        MockitoAnnotations.initMocks(this);
        when(clusterServer.amILeader()).thenReturn(true);
    }


    @Test
    public void stop() throws Exception {
        when(configService.isAlertSystemOn()).thenReturn(true);
        Assert.assertTrue(checker.stop());

        when(configService.isAlertSystemOn()).thenReturn(false);
        Assert.assertFalse(checker.stop());
    }


    @Test
    public void testFutureStart() {
        checker.startFuture();
    }

    @Test
    public void testStartAlert() {
        when(configService.isAlertSystemOn()).thenReturn(false);
        when(clusterServer.amILeader()).thenReturn(true);
        checker.startAlert();
    }
}