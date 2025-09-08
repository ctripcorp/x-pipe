package com.ctrip.xpipe.redis.console.healthcheck.stability;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.collect.Lists;
import io.netty.channel.ConnectTimeoutException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.ctrip.xpipe.redis.console.healthcheck.stability.ConsoleNetworkStabilityInspector.COMMAND_TIMEOUT;
import static com.ctrip.xpipe.redis.console.healthcheck.stability.ConsoleNetworkStabilityInspector.CONNECT_TIMEOUT;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class NetworkStabilityInspectorTest {

    @Mock
    private ConsoleConfig config;

    @Mock
    private MetaCache metaCache;

    @Mock
    private ConsoleServiceManager consoleServiceManager;

    private static final String JQ = "jq";
    private static final String OY = "oy";
    private static final String ALI = "ali";

    private static ExecutorService executor = Executors.newFixedThreadPool(5);

    @AfterClass
    public static void afterAll() {
        executor.shutdown();
    }

    @Test
    public void testIsolated() throws Exception {
        when(config.getQuorum()).thenReturn(2);
        ConsoleNetworkStabilityInspector inspector = new ConsoleNetworkStabilityInspector(metaCache, config, consoleServiceManager);
        when(config.getDcIsolated()).thenReturn(true);

        //config true
        inspector.inspect();
        Assert.assertTrue(inspector.isolated());
        verify(metaCache, never()).currentRegionDcs();

        //config false
        when(config.getDcIsolated()).thenReturn(false);
        inspector.inspect();
        Assert.assertFalse(inspector.isolated());
        verify(metaCache, never()).currentRegionDcs();

        //no config
        when(config.getDcIsolated()).thenReturn(null);
        //other dcs in current region less than quorum
        when(metaCache.currentRegionDcs()).thenReturn(Lists.newArrayList(JQ, OY));
        inspector.inspect();
        Assert.assertFalse(inspector.isolated());
        verify(metaCache, times(1)).currentRegionDcs();
        verify(consoleServiceManager, never()).connectDc(any(), anyInt());


        //has enough other dcs in current region
        when(config.getIsolateAfterRounds()).thenReturn(2);
        when(config.getIsolateRecoverAfterRounds()).thenReturn(1);
        when(metaCache.currentRegionDcs()).thenReturn(Lists.newArrayList(JQ, OY, ALI));

        // all connected failed
        doAnswer(invocation -> new AbstractCommand<Boolean>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setFailure(new ConnectTimeoutException("test"));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return "";
            }
        }.execute(executor)).when(consoleServiceManager).connectDc(any(), anyInt());

        // after 1 rounds
        inspector.inspect();
        Assert.assertFalse(inspector.isolated());

        when(metaCache.currentRegionDcs()).thenReturn(Lists.newArrayList(JQ, OY, ALI));
        // command timeout, result not enough, ignore
        doAnswer(invocation -> new AbstractCommand<Boolean>() {
            @Override
            protected void doExecute() throws Throwable {
                Thread.sleep(COMMAND_TIMEOUT + 100);
                future().setSuccess(false);
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return "";
            }
        }.execute(executor)).when(consoleServiceManager).connectDc(OY, CONNECT_TIMEOUT);
        inspector.inspect();
        Assert.assertFalse(inspector.isolated());

        when(metaCache.currentRegionDcs()).thenReturn(Lists.newArrayList(JQ, OY, ALI));
        // all connected failed
        doAnswer(invocation -> new AbstractCommand<Boolean>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setFailure(new ConnectTimeoutException("test"));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return "";
            }
        }.execute(executor)).when(consoleServiceManager).connectDc(anyString(), anyInt());
        //after 2 rounds
        inspector.inspect();
        Assert.assertTrue(inspector.isolated());

        when(metaCache.currentRegionDcs()).thenReturn(Lists.newArrayList(JQ, OY, ALI));
        //1 dc connected
        doAnswer(invocation -> new AbstractCommand<Boolean>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(true);
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return "";
            }
        }.execute()).when(consoleServiceManager).connectDc(OY, CONNECT_TIMEOUT);
        //after 1 round
        inspector.inspect();
        Assert.assertFalse(inspector.isolated());

    }


}


