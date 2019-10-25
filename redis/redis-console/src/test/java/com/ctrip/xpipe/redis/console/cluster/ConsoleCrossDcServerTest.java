package com.ctrip.xpipe.redis.console.cluster;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Map;
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
    private ConsoleConfig consoleConfig;

    private ConsoleCrossDcServer crossDcClusterServer;

    private int checkIntervalMilli = 10;

    private AtomicBoolean siteLeader = new AtomicBoolean(true);

    private ConsoleLeaderElector consoleLeaderElector;

    @Before
    public void beforeConsoleCrossDcServerTest(){
        crossDcClusterServer = new ConsoleCrossDcServer();
        crossDcClusterServer.setStartIntervalMilli(10);

        crossDcClusterServer.setCheckIntervalMilli(checkIntervalMilli);
        consoleLeaderElector = mock(ConsoleLeaderElector.class);
        when(consoleLeaderElector.amILeader()).thenReturn(siteLeader.get());
        crossDcClusterServer.setConsoleLeaderElector(consoleLeaderElector);

        crossDcClusterServer.setConsoleConfig(consoleConfig);

    }

    @Test
    public void testNonSiteLeader() throws Exception {
        crossDcClusterServer.setCheckIntervalMilli(1);
        when(consoleLeaderElector.amILeader()).thenReturn(false);
        crossDcClusterServer.start();
        Assert.assertFalse(consoleLeaderElector.amILeader());
        Thread.sleep(2);
        Assert.assertFalse(crossDcClusterServer.amILeader());
    }

    @Test
    public void testValidLeader() throws Exception {
        crossDcClusterServer.setCheckIntervalMilli(1);
        when(consoleLeaderElector.amILeader()).thenReturn(true);
        when(consoleConfig.getDatabaseDomainName()).thenReturn("localhost");
        Map<String, String> ipDcMap = Maps.newHashMap();
        ipDcMap.put("127.0.0.1", FoundationService.DEFAULT.getDataCenter());
        ipDcMap.put("localhost", FoundationService.DEFAULT.getDataCenter());
        when(consoleConfig.getDatabaseIpAddresses()).thenReturn(ipDcMap);
        crossDcClusterServer.start();
        Assert.assertTrue(consoleLeaderElector.amILeader());
        waitConditionUntilTimeOut(() -> crossDcClusterServer.amILeader());
    }

    @Test
    public void testNotLeader() throws Exception {
        crossDcClusterServer.setCheckIntervalMilli(1);
        when(consoleLeaderElector.amILeader()).thenReturn(true);
        when(consoleConfig.getDatabaseDomainName()).thenReturn("localhost");
        Map<String, String> ipDcMap = Maps.newHashMap();
        ipDcMap.put("127.0.0.1", "non-exists");
        ipDcMap.put("localhost", "non-exists");
        when(consoleConfig.getDatabaseIpAddresses()).thenReturn(ipDcMap);
        crossDcClusterServer.start();
        Assert.assertTrue(consoleLeaderElector.amILeader());
        sleep(10);
        waitConditionUntilTimeOut(() -> !crossDcClusterServer.amILeader());
    }


    @Test
    public void testLeaderBecomeCrossDcLeader() throws Exception {
        when(consoleLeaderElector.amILeader()).thenReturn(true);
        when(consoleConfig.getDatabaseDomainName()).thenReturn("localhost");
        Map<String, String> ipDcMap = Maps.newHashMap();
        ipDcMap.put("127.0.0.1", FoundationService.DEFAULT.getDataCenter());
        ipDcMap.put("localhost", FoundationService.DEFAULT.getDataCenter());
        when(consoleConfig.getDatabaseIpAddresses()).thenReturn(ipDcMap);
        crossDcClusterServer.isleader();

        waitConditionUntilTimeOut(() -> crossDcClusterServer.amILeader());

    }

    @Test
    public void testNotLeaderTwo() throws Exception {
        when(consoleLeaderElector.amILeader()).thenReturn(true);
        when(consoleConfig.getDatabaseDomainName()).thenReturn("localhost");
        Map<String, String> ipDcMap = Maps.newHashMap();
        ipDcMap.put("127.0.0.1", FoundationService.DEFAULT.getDataCenter());
        ipDcMap.put("localhost", FoundationService.DEFAULT.getDataCenter());
        when(consoleConfig.getDatabaseIpAddresses()).thenReturn(ipDcMap);
        crossDcClusterServer.isleader();

        waitConditionUntilTimeOut(() -> crossDcClusterServer.amILeader());

        crossDcClusterServer.notLeader();

        waitConditionUntilTimeOut(() -> !crossDcClusterServer.amILeader(), 300);
        Assert.assertFalse(crossDcClusterServer.amILeader());

    }


    @Test
    public void testLeaderBecomeCrossDcLeaderThenFallback() throws Exception {

        when(consoleLeaderElector.amILeader()).thenReturn(true);
        when(consoleConfig.getDatabaseDomainName()).thenReturn("localhost");
        Map<String, String> ipDcMap = Maps.newHashMap();
        ipDcMap.put("127.0.0.1", FoundationService.DEFAULT.getDataCenter());
        ipDcMap.put("localhost", FoundationService.DEFAULT.getDataCenter());
        when(consoleConfig.getDatabaseIpAddresses()).thenReturn(ipDcMap);
        crossDcClusterServer.isleader();

        waitConditionUntilTimeOut(() -> crossDcClusterServer.amILeader());
        ipDcMap = Maps.newHashMap();
        ipDcMap.put("127.0.0.1", "not-exist");
        ipDcMap.put("localhost", "not-exist");
        when(consoleConfig.getDatabaseIpAddresses()).thenReturn(ipDcMap);

        waitConditionUntilTimeOut(() -> !crossDcClusterServer.amILeader());
    }


    @After
    public void afterConsoleCrossDcServerTest() throws Exception {
        crossDcClusterServer.stop();
    }

}
