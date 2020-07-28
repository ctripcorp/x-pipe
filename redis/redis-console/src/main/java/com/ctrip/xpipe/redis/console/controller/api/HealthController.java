package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.healthcheck.*;
import com.ctrip.xpipe.redis.console.healthcheck.actions.delay.CrossMasterDelayService;
import com.ctrip.xpipe.redis.console.healthcheck.actions.delay.DelayService;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.DefaultDelayPingActionCollector;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.console.model.consoleportal.UnhealthyInfoModel;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

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

    @Autowired
    private DelayService delayService;

    @Autowired
    private CrossMasterDelayService crossMasterDelayService;

    @RequestMapping(value = "/health/{ip}/{port}", method = RequestMethod.GET)
    public HEALTH_STATE getHealthState(@PathVariable String ip, @PathVariable int port) {

        return defaultDelayPingActionCollector.getState(new HostPort(ip, port));
    }

    @RequestMapping(value = "/redis/delay/{redisIp}/{redisPort}", method = RequestMethod.GET)
    public Long getReplDelayMillis(@PathVariable String redisIp, @PathVariable int redisPort) {
        return delayService.getDelay(new HostPort(redisIp, redisPort));
    }


    @RequestMapping(value = "/redis/inner/delay/{redisIp}/{redisPort}", method = RequestMethod.GET)
    public Long getInnerReplDelayMillis(@PathVariable String redisIp, @PathVariable int redisPort) {
        return delayService.getLocalCachedDelay(new HostPort(redisIp, redisPort));
    }

    @RequestMapping(value = "/redis/inner/delay/all", method = RequestMethod.GET)
    public Map<HostPort, Long> getAllInnerReplDelayMills() {
        return delayService.getDcCachedDelay(FoundationService.DEFAULT.getDataCenter());
    }

    @RequestMapping(value = "/redis/inner/unhealthy", method = RequestMethod.GET)
    public UnhealthyInfoModel getActiveClusterUnhealthyRedis() {
        return delayService.getDcActiveClusterUnhealthyInstance(FoundationService.DEFAULT.getDataCenter());
    }

    @RequestMapping(value = "/redis/inner/unhealthy/all", method = RequestMethod.GET)
    public UnhealthyInfoModel getAllUnhealthyRedis() {
        return delayService.getAllUnhealthyInstance();
    }

    @RequestMapping(value = "/cross-master/delay/{dcId}/" + CLUSTER_ID_PATH_VARIABLE + "/" + SHARD_ID_PATH_VARIABLE, method = RequestMethod.GET)
    public Map<String, Pair<HostPort, Long>> getCrossMasterDelay(@PathVariable String dcId, @PathVariable String clusterId, @PathVariable String shardId) {
        return crossMasterDelayService.getPeerMasterDelayFromSourceDc(dcId, clusterId, shardId);
    }

    @RequestMapping(value = "/cross-master/inner/delay/" + CLUSTER_ID_PATH_VARIABLE + "/" + SHARD_ID_PATH_VARIABLE, method = RequestMethod.GET)
    public Map<String, Pair<HostPort, Long>> getInnerCrossMasterDelay(@PathVariable String clusterId, @PathVariable String shardId) {
        return crossMasterDelayService.getPeerMasterDelayFromCurrentDc(clusterId, shardId);
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
            for(Object controller : ((AbstractHealthCheckAction) action).getControllers()) {
                actionModel.addController(controller.toString());
            }
            model.addAction(actionModel);
        }
        return Codec.DEFAULT.encode(model);
    }

    protected class RedisHealthCheckInstanceModel {

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
        private List<String> controllers;

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
