package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.health.HealthChecker;
import com.ctrip.xpipe.redis.console.health.action.AllMonitorCollector;
import com.ctrip.xpipe.redis.console.health.action.HEALTH_STATE;
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
    private AllMonitorCollector allMonitorCollector;

    @RequestMapping(value = "/health/{ip}/{port}", method = RequestMethod.GET)
    public HEALTH_STATE getHealthState(@PathVariable String ip, @PathVariable int port) {

        HEALTH_STATE state = allMonitorCollector.getState(new HostPort(ip, port));
        return state;
    }


}
