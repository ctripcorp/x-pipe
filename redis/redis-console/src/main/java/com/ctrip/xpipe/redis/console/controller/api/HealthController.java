package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.healthcheck.*;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.DefaultDelayPingActionCollector;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.HEALTH_STATE;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

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
    private DefaultDelayPingActionCollector defaultDelayPingActionCollector;

    @Autowired
    private HealthCheckInstanceManager instanceManager;

    @RequestMapping(value = "/health/{ip}/{port}", method = RequestMethod.GET)
    public HEALTH_STATE getHealthState(@PathVariable String ip, @PathVariable int port) {

        return defaultDelayPingActionCollector.getState(new HostPort(ip, port));
    }

    @RequestMapping(value = "/health/check/instance/{ip}/{port}", method = RequestMethod.GET)
    public String getHealthCheckInstance(@PathVariable String ip, @PathVariable int port) {
        RedisHealthCheckInstance instance = instanceManager.findRedisHealthCheckInstance(new HostPort(ip, port));
        if(instance == null) {
            return "Not found";
        }
        RedisHealthCheckInstanceModel model = new RedisHealthCheckInstanceModel(instance.toString());
        for(HealthCheckAction action : instance.getHealthCheckActions()) {
            HealthCheckActionModel actionModel = new HealthCheckActionModel(action.toString());
            for(Object listener : ((AbstractHealthCheckAction) action).getListeners()) {
                actionModel.addListener(listener.toString());
            }
            model.addAction(actionModel);
        }
        return Codec.DEFAULT.encode(model);
    }

    private class RedisHealthCheckInstanceModel {

        private String info;

        private List<HealthCheckActionModel> actions;

        public RedisHealthCheckInstanceModel(String info) {
            this.info = info;
            this.actions = Lists.newArrayList();
        }

        public void addAction(HealthCheckActionModel action) {
            this.actions.add(action);
        }

        public List<HealthCheckActionModel> getActions() {
            return actions;
        }

        public String getInfo() {
            return info;
        }
    }

    private class HealthCheckActionModel {
        private String name;
        private List<String> listeners;

        public HealthCheckActionModel(String name) {
            this.name = name;
            this.listeners = Lists.newArrayList();
        }

        public void addListener(String listener) {
            listeners.add(listener);
        }

        public String getName() {
            return name;
        }

        public List<String> getListeners() {
            return listeners;
        }
    }
}
