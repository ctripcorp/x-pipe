package com.ctrip.xpipe.redis.console.console.impl;

import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 26, 2018
 */
public class DefaultConsoleServiceTest extends AbstractConsoleTest {

    @Test
    public void testManual(){

        DefaultConsoleService defaultConsoleService = new DefaultConsoleService("http://10.2.45.29");
        HEALTH_STATE instanceStatus = defaultConsoleService.getInstanceStatus("10.2.24.215", 6379);
        logger.info("{}", instanceStatus);

    }

}
