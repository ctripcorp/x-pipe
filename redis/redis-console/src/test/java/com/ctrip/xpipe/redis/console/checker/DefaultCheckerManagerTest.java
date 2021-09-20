package com.ctrip.xpipe.redis.console.checker;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.model.CheckerRole;
import com.ctrip.xpipe.redis.checker.model.CheckerStatus;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * @author lishanglin
 * date 2021/3/21
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultCheckerManagerTest extends AbstractConsoleTest {

    private DefaultCheckerManager manager;

    @Mock
    private ConsoleConfig config;

    private HostPort checkerHostPort = new HostPort("10.0.0.1", 8080);

    @Before
    public void setupDefaultCheckerManagerTest() {
        Mockito.when(config.getClusterDividedParts()).thenReturn(1);
        Mockito.when(config.getCheckerAckTimeoutMilli()).thenReturn(10000);
        manager = new DefaultCheckerManager(config);
    }

    @Test
    public void testRefreshCheckerStatus() {
        manager.expireCheckers();
        manager.refreshCheckerStatus(mockCheckerStatus(0, CheckerRole.FOLLOWER));
        Assert.assertEquals(CheckerRole.FOLLOWER, manager.getCheckers().get(0).get(checkerHostPort).getCheckerRole());
        manager.refreshCheckerStatus(mockCheckerStatus(0, CheckerRole.LEADER));
        manager.expireCheckers();
        Assert.assertEquals(CheckerRole.LEADER, manager.getCheckers().get(0).get(checkerHostPort).getCheckerRole());
    }

    @Test
    public void testPartsChange() {
        Mockito.when(config.getClusterDividedParts()).thenReturn(2);
        manager.expireCheckers();
        Assert.assertEquals(2, manager.getCheckers().size());
        Mockito.when(config.getClusterDividedParts()).thenReturn(1);
        manager.expireCheckers();
        Assert.assertEquals(1, manager.getCheckers().size());
    }

    @Test
    public void testExpireCheckers() {
        manager.expireCheckers();
        Mockito.when(config.getCheckerAckTimeoutMilli()).thenReturn(0);
        manager.refreshCheckerStatus(mockCheckerStatus(0, CheckerRole.FOLLOWER));
        Assert.assertEquals(1, manager.getCheckers().get(0).size());
        manager.expireCheckers();
        Assert.assertEquals(0, manager.getCheckers().get(0).size());
    }

    @Test
    public void testRefreshOutbound() {
        manager.expireCheckers();
        manager.refreshCheckerStatus(mockCheckerStatus(1, CheckerRole.FOLLOWER));
        Assert.assertEquals(1, manager.getCheckers().size());
        Assert.assertTrue(manager.getCheckers().get(0).isEmpty());
    }

    public CheckerStatus mockCheckerStatus(int partIndex, CheckerRole role) {
        CheckerStatus status = new CheckerStatus();
        status.setPartIndex(partIndex);
        status.setCheckerRole(role);
        status.setHostPort(checkerHostPort);
        return status;
    }

}
