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

public class KeeperBalanceInfoCollectOffCheckerTest extends AbstractConsoleIntegrationTest {

    @Autowired
    @InjectMocks
    private KeeperBalanceInfoCollectOffChecker checker;

    @Mock
    private ConfigService configService;

    @Mock
    private CrossDcClusterServer clusterServer;

    @Before
    public void beforeKeeperBalanceInfoCollectOffCheckerTest() {
        MockitoAnnotations.initMocks(this);
        when(clusterServer.amILeader()).thenReturn(true);
    }

    @Test
    public void testStop() {
        when(configService.isKeeperBalanceInfoCollectOn()).thenReturn(true);
        Assert.assertTrue(checker.stop());

        when(configService.isKeeperBalanceInfoCollectOn()).thenReturn(false);
        Assert.assertFalse(checker.stop());
    }

    @Test
    public void testAlert() {
        when(configService.isKeeperBalanceInfoCollectOn()).thenReturn(false);
        when(clusterServer.amILeader()).thenReturn(true);
        checker.startAlert();
    }

}
