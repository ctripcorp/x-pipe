package com.ctrip.xpipe.redis.console.console.impl;

import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 26, 2018
 */
public class DefaultConsoleServiceTest extends AbstractConsoleTest {

    @Test
    @Ignore
    public void testManual(){

        DefaultConsoleService defaultConsoleService = new DefaultConsoleService("http://10.2.45.29",8080);
        HEALTH_STATE instanceStatus = defaultConsoleService.getInstanceStatus("10.2.24.215", 6379);
        logger.info("{}", instanceStatus);

    }


    @Test
    public void testAddress() {

        DefaultConsoleService ipConsoleService = new DefaultConsoleService("http://10.2.45.29", 8080);
        Assert.assertEquals("http://10.2.45.29:8080", ipConsoleService.toString());

        DefaultConsoleService ipConsoleServiceNoHttp = new DefaultConsoleService("http://10.2.45.29", 8080);
        Assert.assertEquals("http://10.2.45.29:8080", ipConsoleServiceNoHttp.toString());

        DefaultConsoleService domainConsoleService = new DefaultConsoleService("http://domain", 80);
        Assert.assertEquals("http://domain", domainConsoleService.toString());

        DefaultConsoleService domainConsoleServiceNoHttp= new DefaultConsoleService("http://domain", 80);
        Assert.assertEquals("http://domain", domainConsoleServiceNoHttp.toString());

    }

}
