package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.health.HealthChecker;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.console.healthcheck.action.HEALTH_STATE;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 07, 2017
 */
@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
@ConditionalOnProperty(name = { HealthChecker.ENABLED }, matchIfMissing = true)
public class HealthController extends AbstractConsoleController{

    @Autowired
    private HealthCheckInstanceManager instanceManager;

    @RequestMapping(value = "/health/{ip}/{port}", method = RequestMethod.GET)
    public HEALTH_STATE getHealthState(@PathVariable String ip, @PathVariable int port) {

        HEALTH_STATE state = instanceManager.findRedisHealthCheckInstance(new HostPort(ip, port)).getHealthCheckContext()
                .getDelayContext().getHealthState();
        return state;
    }

    @RequestMapping(value = "/ping/{ip}/{port}", method = RequestMethod.GET)
    public Boolean getPingStatus(@PathVariable String ip, @PathVariable int port) {

        Boolean result = instanceManager.findRedisHealthCheckInstance(new HostPort(ip, port)).getHealthCheckContext()
                .getPingContext().isHealthy();
        return result;
    }

}
