package com.ctrip.xpipe.redis.console.console.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.checker.controller.result.ActionContextRetMessage;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo.InfoActionContext;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderElector;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.ConsoleService;
import com.ctrip.xpipe.redis.console.exception.NotEnoughResultsException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

import static org.mockito.Mockito.*;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 07, 2017
 */
@RunWith(MockitoJUnitRunner.class)
public class ConsoleServiceManagerTest extends AbstractConsoleTest{

    @Mock
    private ConsoleConfig consoleConfig;

    @Mock
    private ConsoleLeaderElector consoleLeaderElector;

    @Test
    public void test(){

        when(consoleLeaderElector.getAllServers()).thenReturn(Arrays.asList("127.0.0.1"));
        when(consoleConfig.getQuorum()).thenReturn(1);

        ConsoleServiceManager manager = new ConsoleServiceManager(consoleConfig, consoleLeaderElector);

        List<HEALTH_STATE> health_states = manager.getHealthStates("127.0.0.1", 6379);

        logger.info("{}", health_states);
        boolean result = manager.quorumSatisfy(health_states, (state) -> state == HEALTH_STATE.DOWN || state == HEALTH_STATE.UNHEALTHY);
        logger.info("{}", result);
    }

    @Test
    public void testMulti(){

        when(consoleLeaderElector.getAllServers()).thenReturn(Arrays.asList("127.0.0.1"));
        ConsoleServiceManager manager = new ConsoleServiceManager(consoleConfig, consoleLeaderElector);

        List<HEALTH_STATE> health_states = manager.getHealthStates("127.0.0.1", 6379);
        logger.info("{}", health_states);

    }

    @Test
    public void testGetServiceByDcIgnoreCase() throws Exception {
        when(consoleConfig.getConsoleDomains()).thenReturn(new HashMap<String, String>(){{
            put("jq", "http://127.0.0.1:8080");
            put("OY", "http://127.0.0.1:8081");
        }});

        ConsoleServiceManager manager = new ConsoleServiceManager(consoleConfig, consoleLeaderElector);
        Method method = ConsoleServiceManager.class.getDeclaredMethod("getServiceByDc", String.class);
        method.setAccessible(true);

        ConsoleService consoleService = (ConsoleService) method.invoke(manager, "jq");
        Assert.assertNotNull(consoleService);

        consoleService = (ConsoleService) method.invoke(manager, "oy");
        Assert.assertNotNull(consoleService);

        try {
            method.invoke(manager, "rb");
            Assert.fail();
        } catch (Exception e) {
            logger.info("[testGetServiceByDc] get unknown dc", e);
        }
    }

    @Test
    public void getAllDcIsolatedCheckResultTest() throws Exception {
        ConsoleServiceManager manager = spy(new ConsoleServiceManager(consoleConfig, consoleLeaderElector));
        HostPort hostPort1 = new HostPort("127.0.0.1", 8080);
        HostPort hostPort2 = new HostPort("127.0.0.2", 8080);
        HostPort hostPort3 = new HostPort("127.0.0.3", 8080);

        Map<String, ConsoleService> allConsoleServices = new HashMap<>();
        ConsoleService server1 = mock(ConsoleService.class);
        ConsoleService server2 = mock(ConsoleService.class);
        ConsoleService server3 = mock(ConsoleService.class);
        when(server1.getInnerDcIsolated()).thenReturn(true);
        when(server2.getInnerDcIsolated()).thenReturn(null);
        when(server3.getInnerDcIsolated()).thenReturn(true);
        allConsoleServices.put(hostPort1.toString(), server1);
        allConsoleServices.put(hostPort2.toString(), server2);
        allConsoleServices.put(hostPort3.toString(), server3);
        doReturn(allConsoleServices).when(manager).loadAllConsoleServices();

        try {
            manager.getAllDcIsolatedCheckResult();
            Assert.fail();
        } catch (Throwable th) {
            Assert.assertTrue(th instanceof NotEnoughResultsException);
        }

        doThrow(new XpipeRuntimeException("test")).when(server2).getInnerDcIsolated();
        try {
            manager.getAllDcIsolatedCheckResult();
            Assert.fail();
        } catch (Throwable th) {
            Assert.assertTrue(th instanceof NotEnoughResultsException);
        }

        doReturn(false).when(server2).getInnerDcIsolated();
        try {
            Map<String, Boolean> result = manager.getAllDcIsolatedCheckResult();
            Assert.assertEquals(3, result.size());
            Assert.assertTrue(result.get(hostPort1.toString()));
            Assert.assertFalse(result.get(hostPort2.toString()));
            Assert.assertTrue(result.get(hostPort3.toString()));
        } catch (Throwable th) {
            Assert.fail();
        }
    }

    @Test
    public void getAllLocalRedisInfos_concurrentSkipFailedDc() throws Exception {
        // good DC returns one host; bad DC throws -> must be skipped, not crash the aggregate
        ConsoleService ok = mock(ConsoleService.class);
        ConsoleService bad = mock(ConsoleService.class);
        HostPort hp = new HostPort("1.1.1.1", 6379);
        when(ok.getAllLocalRedisInfos(any())).thenReturn(
                Collections.singletonMap(hp, new InfoActionContext.Result()));
        when(bad.getAllLocalRedisInfos(any())).thenThrow(new RuntimeException("dc down"));

        ConsoleServiceManager manager = new ConsoleServiceManager(consoleConfig, consoleLeaderElector);
        Field servicesField = ConsoleServiceManager.class.getDeclaredField("services");
        servicesField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, ConsoleService> services = (Map<String, ConsoleService>) servicesField.get(manager);
        services.put("JQ", ok);
        services.put("OY", bad);

        Map<HostPort, ActionContextRetMessage<Map<String, String>>> result =
                manager.getAllLocalRedisInfos(new HashSet<>(Arrays.asList("jq", "oy")), null);

        // bad DC skipped, only good DC's host survives, no exception thrown
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.containsKey(hp));
    }
}
