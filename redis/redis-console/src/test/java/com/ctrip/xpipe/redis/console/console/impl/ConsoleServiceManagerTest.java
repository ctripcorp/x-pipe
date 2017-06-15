package com.ctrip.xpipe.redis.console.console.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.health.action.HEALTH_STATE;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.*;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 07, 2017
 */
@RunWith(MockitoJUnitRunner.class)
public class ConsoleServiceManagerTest extends AbstractConsoleTest{

    @Mock
    private ConsoleConfig consoleConfig;

    @Test
    public void test(){

        when(consoleConfig.getAllConsoles()).thenReturn("127.0.0.1:8080");
        when(consoleConfig.getQuorum()).thenReturn(1);

        ConsoleServiceManager manager = new ConsoleServiceManager();
        manager.setConsoleConfig(consoleConfig);

        List<HEALTH_STATE> health_states = manager.allHealthStatus("127.0.0.1", 6379);

        logger.info("{}", health_states);
        boolean result = manager.quorumSatisfy(health_states, (state) -> state == HEALTH_STATE.DOWN || state == HEALTH_STATE.UNHEALTHY);
        logger.info("{}", result);
    }

    @Test
    public void testMulti(){

        when(consoleConfig.getAllConsoles()).thenReturn("127.0.0.1:8080, 127.0.0.1:8081");
        when(consoleConfig.getQuorum()).thenReturn(1);

        ConsoleServiceManager manager = new ConsoleServiceManager();
        manager.setConsoleConfig(consoleConfig);

        List<HEALTH_STATE> health_states = manager.allHealthStatus("127.0.0.1", 6379);
        logger.info("{}", health_states);

    }
}
