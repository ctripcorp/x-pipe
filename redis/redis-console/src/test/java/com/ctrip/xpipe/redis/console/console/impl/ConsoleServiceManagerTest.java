package com.ctrip.xpipe.redis.console.console.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderElector;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.ConsoleService;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.when;

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
}
