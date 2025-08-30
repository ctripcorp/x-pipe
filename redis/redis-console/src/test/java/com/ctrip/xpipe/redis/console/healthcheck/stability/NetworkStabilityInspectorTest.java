package com.ctrip.xpipe.redis.console.healthcheck.stability;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.collect.Lists;
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
        ConsoleNetworkStabilityInspector inspector = new ConsoleNetworkStabilityInspector(metaCache, config, consoleServiceManager);
        when(config.checkDcNetwork()).thenReturn(false);

        inspector.inspect();
        Assert.assertFalse(inspector.isolated());
        verify(metaCache, never()).regionDcs(any());

        when(config.checkDcNetwork()).thenReturn(true);
        when(metaCache.regionDcs(JQ)).thenReturn(Lists.newArrayList(JQ));
        inspector.inspect();
        Assert.assertFalse(inspector.isolated());
        verify(metaCache, times(1)).regionDcs(any());
        verify(consoleServiceManager, never()).connectDc(any(), anyInt());


        when(config.getIsolateAfterRounds()).thenReturn(2);
        when(config.getIsolateRecoverAfterRounds()).thenReturn(1);
        when(metaCache.regionDcs(JQ)).thenReturn(Lists.newArrayList(JQ, OY, ALI));

        doAnswer(invocation -> new AbstractCommand<Boolean>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(false);
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return "";
            }
        }.execute(executor)).when(consoleServiceManager).connectDc(any(), anyInt());

        // all connected failed after 1 rounds
        inspector.inspect();
        Assert.assertFalse(inspector.isolated());

        // command timeout, result not enough
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

        // all connected failed after 2 rounds
        doAnswer(invocation -> new AbstractCommand<Boolean>() {
            @Override
            protected void doExecute() throws Throwable {
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
        Assert.assertTrue(inspector.isolated());


        //1 dc connected after 1 round
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
        inspector.inspect();
        Assert.assertFalse(inspector.isolated());

    }


}


