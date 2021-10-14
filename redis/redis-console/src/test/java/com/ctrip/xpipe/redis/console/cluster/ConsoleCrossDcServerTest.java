package com.ctrip.xpipe.redis.console.cluster;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.election.CrossDcLeaderElectionAction;
import com.ctrip.xpipe.redis.console.exception.DalUpdateException;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.utils.DateTimeUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 14, 2017
 */
@RunWith(MockitoJUnitRunner.class)
public class ConsoleCrossDcServerTest extends AbstractConsoleTest{


    @Mock
    private ConfigService configService;

    private ConsoleCrossDcServer crossDcClusterServer;

    private AtomicBoolean siteLeader = new AtomicBoolean(true);

    private ConsoleLeaderElector consoleLeaderElector;

    private volatile String leaderDc = "init-dc";

    private String currentDc = FoundationService.DEFAULT.getDataCenter();

    private volatile int electInterval = 10;

    @Before
    public void beforeConsoleCrossDcServerTest() throws Exception {
        crossDcClusterServer = new ConsoleCrossDcServer();

        electInterval = 10;
        consoleLeaderElector = mock(ConsoleLeaderElector.class);
        crossDcClusterServer.setConsoleLeaderElector(consoleLeaderElector);
        crossDcClusterServer.setCrossDcLeaderElectionAction(new TestCrossDcLeaderElectionAction());
        crossDcClusterServer.setConfigService(configService);

        when(configService.getCrossDcLeader()).thenAnswer(inv -> leaderDc);
        when(consoleLeaderElector.amILeader()).thenAnswer(inv -> siteLeader.get());

        // db can not write with network island
        Mockito.doAnswer(inv -> { throw new DalUpdateException("db write is not available"); })
                .when(configService).updateCrossDcLeader(Mockito.any(), Mockito.any());
    }

    @Test
    public void testNonSiteLeader() throws Exception {
        siteLeader.set(false);
        leaderDc = currentDc;

        crossDcClusterServer.start();
        Assert.assertFalse(consoleLeaderElector.amILeader());

        sleep(electInterval);
        Assert.assertFalse(crossDcClusterServer.amILeader());
    }

    @Test
    public void testValidLeader() throws Exception {
        siteLeader.set(true);
        leaderDc = currentDc;
        crossDcClusterServer.start();

        Assert.assertTrue(consoleLeaderElector.amILeader());
        waitConditionUntilTimeOut(() -> crossDcClusterServer.amILeader());
    }

    @Test
    public void testNotLeader() throws Exception {
        siteLeader.set(true);

        crossDcClusterServer.start();
        Assert.assertTrue(consoleLeaderElector.amILeader());

        sleep(electInterval);
        waitConditionUntilTimeOut(() -> !crossDcClusterServer.amILeader());
    }


    @Test
    public void testLeaderBecomeCrossDcLeader() {
        siteLeader.set(true);
        leaderDc = currentDc;

        crossDcClusterServer.isleader();
        sleep(electInterval);
        Assert.assertTrue(crossDcClusterServer.amILeader());

    }

    @Test
    public void testNotLeaderTwo() throws Exception {
        siteLeader.set(true);
        leaderDc = currentDc;

        crossDcClusterServer.isleader();
        waitConditionUntilTimeOut(() -> crossDcClusterServer.amILeader());

        crossDcClusterServer.notLeader();
        waitConditionUntilTimeOut(() -> !crossDcClusterServer.amILeader(), 300);
        Assert.assertFalse(crossDcClusterServer.amILeader());

    }


    @Test
    public void testLeaderBecomeCrossDcLeaderThenFallback() throws Exception {
        siteLeader.set(true);
        leaderDc = currentDc;

        crossDcClusterServer.isleader();
        waitConditionUntilTimeOut(() -> crossDcClusterServer.amILeader());

        leaderDc = "other-dc";
        sleep(electInterval * 2);
        Assert.assertFalse(crossDcClusterServer.amILeader());
    }

    @Test
    public void testForceSetLeader() {
        siteLeader.set(true);

        crossDcClusterServer.isleader();
        sleep(2 * electInterval);
        Assert.assertFalse(crossDcClusterServer.amILeader());

        ConfigModel config = new ConfigModel();
        config.setVal(currentDc);

        // no handle force set leader on election handling
        int originInterval = electInterval;
        electInterval = 100;

        try {
            sleep(originInterval); // wait for current election done
            crossDcClusterServer.forceSetCrossLeader(config, DateTimeUtils.getSecondsLaterThan(new Date(), 100));
        } catch (Exception e) {
            e.printStackTrace();
        }

        sleep(2 * electInterval);
        Assert.assertTrue(crossDcClusterServer.amILeader());

        electInterval = originInterval;
    }

    @Test
    public void testForceSetNotLeader() throws Exception {
        siteLeader.set(true);
        leaderDc = currentDc;

        crossDcClusterServer.isleader();
        waitConditionUntilTimeOut(() -> crossDcClusterServer.amILeader());

        ConfigModel config = new ConfigModel();
        config.setVal("other-dc");

        try {
            crossDcClusterServer.forceSetCrossLeader(config, DateTimeUtils.getSecondsLaterThan(new Date(), 100));
        } catch (Exception e) {
            e.printStackTrace();
        }

        sleep(2 * electInterval);
        Assert.assertFalse(crossDcClusterServer.amILeader());
    }

    @Test
    public void testRefreshLeader() throws Exception {
        int originElectInterval = electInterval;
        siteLeader.set(true);
        leaderDc = currentDc;

        crossDcClusterServer.isleader();
        waitConditionUntilTimeOut(() -> crossDcClusterServer.amILeader());

        electInterval = 10000;
        sleep(originElectInterval);

        leaderDc = "other-dc";
        sleep(2 * originElectInterval);
        Assert.assertTrue(crossDcClusterServer.amILeader());

        crossDcClusterServer.refreshCrossLeaderStatus();
        Assert.assertFalse(crossDcClusterServer.amILeader());
    }


    @After
    public void afterConsoleCrossDcServerTest() throws Exception {
        crossDcClusterServer.stop();
    }

    private class TestCrossDcLeaderElectionAction extends CrossDcLeaderElectionAction {

        public TestCrossDcLeaderElectionAction() {
            super(null, null, Mockito.mock(ConsoleConfig.class));
        }

        protected boolean shouldElect() {
            return true;
        }

        protected void beforeElect() {
        }

        protected void doElect() {
        }

        protected void afterElect() {
            notifyObservers(leaderDc);
        }

        protected long getElectIntervalMillSecond() {
            return electInterval;
        }
    }

}
