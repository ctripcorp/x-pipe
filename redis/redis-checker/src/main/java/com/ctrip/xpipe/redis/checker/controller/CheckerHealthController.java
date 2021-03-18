package com.ctrip.xpipe.redis.checker.controller;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DefaultDelayPingActionCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * @author lishanglin
 * date 2021/3/8
 */
@RestController
@RequestMapping("/api")
@ConditionalOnProperty(name = { HealthChecker.ENABLED }, matchIfMissing = true)
public class CheckerHealthController {

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
        HealthCheckInstanceModel model = buildHealthCheckInfo(instance);
        return Codec.DEFAULT.encode(model);
    }

    @RequestMapping(value = "/health/check/cluster/{clusterId}", method = RequestMethod.GET)
    public String getClusterHealthCheckInstance(@PathVariable String clusterId) {
        ClusterHealthCheckInstance instance = instanceManager.findClusterHealthCheckInstance(clusterId);
        if(instance == null) {
            return "Not found";
        }
        HealthCheckInstanceModel model = buildHealthCheckInfo(instance);
        return Codec.DEFAULT.encode(model);
    }

    private HealthCheckInstanceModel buildHealthCheckInfo(HealthCheckInstance<?> instance) {
        HealthCheckInstanceModel model = new HealthCheckInstanceModel(instance.toString());
        for(HealthCheckAction action : instance.getHealthCheckActions()) {
            HealthCheckActionModel actionModel = new HealthCheckActionModel(action.toString());
            for(Object listener : ((AbstractHealthCheckAction) action).getListeners()) {
                actionModel.addListener(listener.toString());
            }
            for(Object controller : ((AbstractHealthCheckAction) action).getControllers()) {
                actionModel.addController(controller.toString());
            }
            model.addAction(actionModel);
        }

        return model;
    }

    public static class HealthCheckInstanceModel {

        private String info;

        private List<HealthCheckActionModel> actions;

        public HealthCheckInstanceModel() {
        }

        public HealthCheckInstanceModel(String info) {
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

    public static class HealthCheckActionModel {
        private String name;
        private List<String> listeners;
        private List<String> controllers;

        public HealthCheckActionModel() {
        }

        public HealthCheckActionModel(String name) {
            this.name = name;
            this.listeners = Lists.newArrayList();
            this.controllers = Lists.newArrayList();
        }

        public void addListener(String listener) {
            listeners.add(listener);
        }

        public void addController(String controller) {
            controllers.add(controller);
        }

        public String getName() {
            return name;
        }

        public List<String> getListeners() {
            return listeners;
        }

        public List<String> getControllers() {
            return controllers;
        }
    }

}
