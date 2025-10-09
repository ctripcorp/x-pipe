package com.ctrip.xpipe.redis.console.healthcheck.stability;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.channel.ConnectTimeoutException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.ctrip.xpipe.redis.console.healthcheck.stability.ConsoleNetworkStabilityInspector.COMMAND_TIMEOUT;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class NetworkStabilityInspectorTest {

    @Mock
    private ConsoleConfig config;

    @Mock
    private MetaCache metaCache;

    private static final String JQ = "jq";
    private static final String OY = "oy";
    private static final String ALI = "ali";
    private static final String JQ_DOMAIN = "http://jq";
    private static final String OY_DOMAIN = "http://oy";
    private static final String ALI_DOMAIN = "http://ali";

    private static ExecutorService executor = Executors.newFixedThreadPool(5);

    @AfterClass
    public static void afterAll() {
        executor.shutdown();
    }

    @Test
    public void testIsolated() throws Exception {
        when(config.getQuorum()).thenReturn(2);
        ConsoleNetworkStabilityInspector inspector = spy(new ConsoleNetworkStabilityInspector(metaCache, config));
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
        verify(config, never()).getConsoleDomains();

        //missing console domains
        when(config.getIsolateAfterRounds()).thenReturn(2);
        when(config.getIsolateRecoverAfterRounds()).thenReturn(1);
        when(metaCache.currentRegionDcs()).thenReturn(Lists.newArrayList(JQ, OY, ALI));
        Map<String,String> missingConsoleDomains = Maps.newHashMap();
        missingConsoleDomains.put(JQ, JQ_DOMAIN);
        missingConsoleDomains.put(OY, OY_DOMAIN);
        when(config.getConsoleDomains()).thenReturn(missingConsoleDomains);
        inspector.inspect();
        Assert.assertFalse(inspector.isolated());
        verify(metaCache, times(2)).currentRegionDcs();
        verify(config, times(3)).getConsoleDomains();
        verify(inspector, never()).connectDcConsole(anyString());


        //has enough other dcs in current region
        Map<String,String> normalConsoleDomains = Maps.newHashMap();
        normalConsoleDomains.put(JQ, JQ_DOMAIN);
        normalConsoleDomains.put(OY, OY_DOMAIN);
        normalConsoleDomains.put(ALI, ALI_DOMAIN);
        when(config.getConsoleDomains()).thenReturn(normalConsoleDomains);

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
        }.execute(executor)).when(inspector).connectDcConsole(anyString());

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
        }.execute(executor)).when(inspector).connectDcConsole(OY_DOMAIN);
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
        }.execute(executor)).when(inspector).connectDcConsole(anyString());
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
        }.execute()).when(inspector).connectDcConsole(OY_DOMAIN);
        //after 1 round
        inspector.inspect();
        Assert.assertFalse(inspector.isolated());

    }


}


