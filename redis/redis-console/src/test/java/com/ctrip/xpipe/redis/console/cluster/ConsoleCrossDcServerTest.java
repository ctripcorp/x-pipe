package com.ctrip.xpipe.redis.console.cluster;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.foundation.DefaultFoundationService;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.ds.XPipeDataSource;
import com.ctrip.xpipe.redis.console.ds.XpipeDataSourceProvider;
import com.ctrip.xpipe.simpleserver.Server;
import com.google.common.collect.Lists;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.unidal.dal.jdbc.datasource.DataSource;
import org.unidal.dal.jdbc.datasource.JdbcDataSourceDescriptor;
import org.unidal.dal.jdbc.datasource.model.entity.DataSourceDef;
import org.unidal.dal.jdbc.datasource.model.entity.DataSourcesDef;
import org.unidal.dal.jdbc.datasource.model.entity.PropertiesDef;
import org.xbill.DNS.TextParseException;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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

    private List<String> cnames = new LinkedList<>();

    private AtomicLong pingDelayMilli = new AtomicLong();

    private AtomicBoolean siteLeader = new AtomicBoolean(true);

    private AtomicReference<List<Long>> pingStats = new AtomicReference<>();

    private AtomicReference<String> url = new AtomicReference<>();

    private ConsoleLeaderElector consoleLeaderElector;

    private ConsoleServiceManager consoleServiceManager;

    @Before
    public void beforeConsoleCrossDcServerTest(){

        crossDcClusterServer = new ConsoleCrossDcServer(){
            @Override
            protected Command<Boolean> getPingCommand(String host, int port) {
                return new AbstractCommand<Boolean>() {
                    @Override
                    protected void doExecute() throws Exception {
                        TimeUnit.MILLISECONDS.sleep(pingDelayMilli.get());
                        future().setSuccess(true);
                    }

                    @Override
                    protected void doReset() {

                    }

                    @Override
                    public String getName() {
                        return "Fake-Ping-Command";
                    }
                };
            }
        };

        crossDcClusterServer.setCheckIntervalMilli(checkIntervalMilli);
        consoleLeaderElector = mock(ConsoleLeaderElector.class);
        when(consoleLeaderElector.amILeader()).thenReturn(siteLeader.get());
        crossDcClusterServer.setConsoleLeaderElector(consoleLeaderElector);

        consoleServiceManager = mock(ConsoleServiceManager.class);
        when(consoleServiceManager.getAllDatabaseAffinity()).thenReturn(pingStats.get());
        crossDcClusterServer.setConsoleServiceManager(consoleServiceManager);

        when(consoleConfig.getCrossDcLeaderPingAddress()).thenReturn(new HostPort());
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
        pingDelayMilli.set(1);
        List<Long> pings = Lists.newArrayList(TimeUnit.SECONDS.toNanos(2), TimeUnit.SECONDS.toNanos(1));
        pingStats.set(pings);
        when(consoleServiceManager.getAllDatabaseAffinity()).thenReturn(pings);
        crossDcClusterServer.start();
        Assert.assertTrue(consoleLeaderElector.amILeader());
        waitConditionUntilTimeOut(() -> crossDcClusterServer.amILeader());
    }

    @Test
    public void testNonOthersLeader() throws Exception {
        crossDcClusterServer.setCheckIntervalMilli(1);
        when(consoleLeaderElector.amILeader()).thenReturn(true);
        pingDelayMilli.set(1);
        List<Long> pings = Lists.newArrayList();
        pingStats.set(pings);
        when(consoleServiceManager.getAllDatabaseAffinity()).thenReturn(pings);
        crossDcClusterServer.start();
        Assert.assertTrue(consoleLeaderElector.amILeader());
        waitConditionUntilTimeOut(() -> crossDcClusterServer.amILeader());
    }


    @Test
    public void testLeaderBecomeCrossDcLeader() throws TimeoutException {

        crossDcClusterServer.isleader();

        waitConditionUntilTimeOut(() -> crossDcClusterServer.amILeader());

    }

    @Test
    public void testNotLeader() throws TimeoutException {

        crossDcClusterServer.isleader();

        waitConditionUntilTimeOut(() -> crossDcClusterServer.amILeader());

        crossDcClusterServer.notLeader();

        waitConditionUntilTimeOut(() -> !crossDcClusterServer.amILeader(), 300);
        Assert.assertFalse(crossDcClusterServer.amILeader());

    }


    @Test
    public void testLeaderBecomeCrossDcLeaderThenFallback() throws TimeoutException {

        crossDcClusterServer.isleader();

        waitConditionUntilTimeOut(() -> crossDcClusterServer.amILeader());

        List<Long> pings = Lists.newArrayList(1L, 2L);
        pingStats.set(pings);
        when(consoleServiceManager.getAllDatabaseAffinity()).thenReturn(pings);

        waitConditionUntilTimeOut(() -> !crossDcClusterServer.amILeader());
    }

    @Test
    public void testCommand() throws Exception {
        Server server = startEchoServer();
        crossDcClusterServer = new ConsoleCrossDcServer();
        int port = server.getPort();
        Command command = crossDcClusterServer.getPingCommand("localhost", port);
        CommandFuture future = command.execute();
        waitConditionUntilTimeOut(()->(future.isDone() && future.isSuccess()));

        server.stop();
        command = crossDcClusterServer.getPingCommand("localhost", port);
        CommandFuture future2 = command.execute();
        waitConditionUntilTimeOut(()->future2.isDone() && !future2.isSuccess());
    }

    @Test
    public void testInitValue() {
        Assert.assertEquals(Long.MAX_VALUE, crossDcClusterServer.getDatabasePingStats());
    }

//    @Ignore
    @Test
    public void testDynamicChange() {
        pingDelayMilli.set(1);
        ConsoleCrossDcServer crossDcClusterServer2 = new ConsoleCrossDcServer(){
            @Override
            protected Command<Boolean> getPingCommand(String host, int port) {
                return new AbstractCommand<Boolean>() {
                    @Override
                    protected void doExecute() throws Exception {
                        TimeUnit.MILLISECONDS.sleep(10);
                        future().setSuccess(true);
                    }

                    @Override
                    protected void doReset() {

                    }

                    @Override
                    public String getName() {
                        return "Fake-Ping-Command";
                    }
                };
            }
        };

        crossDcClusterServer2.setCheckIntervalMilli(checkIntervalMilli);
        consoleLeaderElector = mock(ConsoleLeaderElector.class);
        when(consoleLeaderElector.amILeader()).thenReturn(true);
        crossDcClusterServer2.setConsoleLeaderElector(consoleLeaderElector);

        consoleServiceManager = mock(ConsoleServiceManager.class);
        when(consoleServiceManager.getAllDatabaseAffinity()).thenReturn(Lists.newArrayList(crossDcClusterServer.getDatabasePingStats()));
        crossDcClusterServer2.setConsoleServiceManager(consoleServiceManager);

        when(consoleConfig.getCrossDcLeaderPingAddress()).thenReturn(new HostPort());
        crossDcClusterServer2.setConsoleConfig(consoleConfig);

        crossDcClusterServer2.checkDataBaseCurrentDc();
        Assert.assertTrue(crossDcClusterServer2.amILeader());

        crossDcClusterServer.checkDataBaseCurrentDc();
        crossDcClusterServer2.checkDataBaseCurrentDc();
        Assert.assertTrue(crossDcClusterServer.amILeader());
        Assert.assertFalse(crossDcClusterServer2.amILeader());
    }



    @After
    public void afterConsoleCrossDcServerTest() throws Exception {
        crossDcClusterServer.stop();
    }

}
