package com.ctrip.xpipe.redis.checker.controller;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.RedisInfoManager;
import com.ctrip.xpipe.redis.checker.controller.result.ActionContextRetMessage;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DefaultDelayPingActionCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStatusDesc;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.info.RedisUsedMemoryCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.infoStats.KeeperFlowCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.AbstractRedisConfigRuleAction;
import com.ctrip.xpipe.redis.checker.healthcheck.stability.StabilityHolder;
import com.ctrip.xpipe.redis.checker.impl.KeeperContainerInfoReporter;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardActive;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

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
    private RedisUsedMemoryCollector redisUsedMemoryCollector;

    @Autowired
    private KeeperFlowCollector keeperFlowCollector;

    @Autowired
    private HealthCheckInstanceManager instanceManager;

    @Autowired
    private RedisInfoManager redisInfoManager;

    @Autowired
    private StabilityHolder siteStability;

    @RequestMapping(value = "/health/{ip}/{port}", method = RequestMethod.GET)
    public HEALTH_STATE getHealthState(@PathVariable String ip, @PathVariable int port) {
        if (siteStability.isSiteStable()) return defaultDelayPingActionCollector.getState(new HostPort(ip, port));
        else return HEALTH_STATE.UNKNOWN;
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

    @RequestMapping(value = "/health/check/keeper/{ip}/{port}", method = RequestMethod.GET)
    public String getHealthCheckKeeper(@PathVariable String ip, @PathVariable int port) {
        KeeperHealthCheckInstance instance = instanceManager.findKeeperHealthCheckInstance(new HostPort(ip, port));
        if(instance == null) {
            return "Not found";
        }
        HealthCheckInstanceModel model = buildHealthCheckInfo(instance);
        return Codec.DEFAULT.encode(model);
    }

    @RequestMapping(value = "/health/check/redis-for-assigned-action/{ip}/{port}", method = RequestMethod.GET)
    public String getHealthCheckRedisInstanceForAssignedAction(@PathVariable String ip, @PathVariable int port) {
        RedisHealthCheckInstance instance = instanceManager.findRedisInstanceForAssignedAction(new HostPort(ip, port));
        if(instance == null) {
            return "Not found";
        }
        HealthCheckInstanceModel model = buildHealthCheckInfo(instance);
        return Codec.DEFAULT.encode(model);
    }

    @RequestMapping(value = "/health/redis/info/{ip}/{port}", method = RequestMethod.GET)
    public ActionContextRetMessage<Map<String, String>> getRedisInfo(@PathVariable String ip, @PathVariable int port) {
        return ActionContextRetMessage.from(redisInfoManager.getInfoByHostPort(new HostPort(ip, port)));
    }

    @RequestMapping(value = "/health/redis/info/all", method = RequestMethod.GET)
    public Map<HostPort, ActionContextRetMessage<Map<String, String>>> getAllRedisInfo() {
        return ActionContextRetMessage.map(redisInfoManager.getAllInfos());
    }

    @GetMapping("/health/check/status/all")
    public Map<HostPort, HealthStatusDesc> getAllHealthStatusDesc() {
        if (siteStability.isSiteStable()) return defaultDelayPingActionCollector.getAllHealthStatus();
        else return Collections.emptyMap();
    }

    @GetMapping("/health/keeper/status/all")
    public ConcurrentMap<String, Map<DcClusterShardActive, Long>> getAllKeeperFlows() {
        return keeperFlowCollector.getHostPort2InputFlow();
    }

    @GetMapping("/health/redis/used-memory/all")
    public ConcurrentMap<DcClusterShard, Long> getAllDclusterShardUsedMemory() {
        return redisUsedMemoryCollector.getDcClusterShardUsedMemory();
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
            if(action instanceof AbstractRedisConfigRuleAction) {
                for(Object redisCheckRule : ((AbstractRedisConfigRuleAction) action).getRedisConfigCheckRules()) {
                    actionModel.addRedisCheckRule(redisCheckRule.toString());
                }
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
        private List<String> redisCheckRules;

        public HealthCheckActionModel() {
        }

        public HealthCheckActionModel(String name) {
            this.name = name;
            this.listeners = Lists.newArrayList();
            this.controllers = Lists.newArrayList();
            this.redisCheckRules = Lists.newArrayList();
        }

        public void addListener(String listener) {
            listeners.add(listener);
        }

        public void addController(String controller) {
            controllers.add(controller);
        }

        public void addRedisCheckRule(String redisCheckRule){
            redisCheckRules.add(redisCheckRule);
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

        public List<String> getRedisCheckRules() {
            return redisCheckRules;
        }
    }

}
