package com.ctrip.xpipe.redis.console.healthcheck;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author chen.zhu
 * <p>
 * Sep 04, 2018
 */
public class AbstractHealthCheckTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private HealthCheckInstanceManager instanceManager;


}
