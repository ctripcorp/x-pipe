package com.ctrip.xpipe.redis.console.migration.auto;

import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.api.migration.auto.MonitorServiceFactory;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig;
import com.ctrip.xpipe.redis.console.config.model.BeaconClusterRoute;
import com.ctrip.xpipe.redis.console.config.model.BeaconOrgRoute;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author lishanglin
 * date 2021/1/15
 */
@Service
public class DefaultMonitorManager implements MonitorManager {

    private final MetaCache metaCache;

    private final ConsoleConfig config;

    private Map<Long, DefaultMonitorClusterManager> orgMonitorMap;

    private static final int CONSISTENT_HASH_VIRTUAL_NODE_NUM = 100;

    private static final Logger logger = LoggerFactory.getLogger(DefaultMonitorManager.class);

    @Autowired
    public DefaultMonitorManager(MetaCache metaCache, ConsoleConfig config) {
        this.metaCache = metaCache;
        this.config = config;
        this.init();
    }

    private void init() {
        long checkInterval = this.config.getClusterHealthCheckInterval() / 1000; // 秒
        String serverMode = this.config.getServerMode();
        MetaCache cache = ConsoleServerModeCondition.SERVER_MODE.CHECKER.name().equals(serverMode.toUpperCase()) ?
            metaCache : null;
        List<BeaconOrgRoute> beaconOrgRoutes = this.config.getBeaconOrgRoutes();
        this.orgMonitorMap = beaconOrgRoutes.stream().collect(
            Collectors.toMap(BeaconOrgRoute::getOrgId, orgRoute -> {
                List<MonitorService> monitorServices = orgRoute.getClusterRoutes()
                    .stream()
                    .map(this::constructMonitorService)
                    .collect(Collectors.toList());
                return new DefaultMonitorClusterManager(cache, CONSISTENT_HASH_VIRTUAL_NODE_NUM,
                    monitorServices, checkInterval);
            }));

        // 注册beacon.org.routes配置监听
        this.config.register(Collections.singletonList(DefaultConsoleConfig.KEY_BEACON_ORG_ROUTE),
            ((key, oldValue, newValue) -> {
                logger.info("key: {}, oldValue: {}, newValue{}", key, oldValue, newValue);
                List<BeaconOrgRoute> oldOrgRoutes = JsonCodec.INSTANCE
                    .decode(oldValue, new GenericTypeReference<List<BeaconOrgRoute>>() {});
                List<BeaconOrgRoute> newOrgRoutes = JsonCodec.INSTANCE
                    .decode(newValue, new GenericTypeReference<List<BeaconOrgRoute>>() {});
                if (CollectionUtils.isEmpty(newOrgRoutes)) {
                    logger.error("Monitor Service cannot be null");
                    return;
                }
                if (CollectionUtils.isEmpty(oldOrgRoutes)) {
                    logger.info("first write org routes config, init");
                    // beacon.org.routes首次变更，直接更新
                    this.orgMonitorMap = newOrgRoutes.stream().collect(
                        Collectors.toMap(BeaconOrgRoute::getOrgId, orgRoute -> {
                            List<MonitorService> monitorServices = orgRoute.getClusterRoutes()
                                .stream()
                                .map(this::constructMonitorService)
                                .collect(Collectors.toList());
                            return new DefaultMonitorClusterManager(cache, CONSISTENT_HASH_VIRTUAL_NODE_NUM,
                                monitorServices, checkInterval);
                        }));
                    return;
                }

                Map<Long, Map<String, BeaconClusterRoute>> oldOrgHostRouteMap = oldOrgRoutes.stream()
                    .collect(Collectors.toMap(BeaconOrgRoute::getOrgId, or -> or.getClusterRoutes()
                        .stream().collect(Collectors.toMap(BeaconClusterRoute::getName, Function.identity()))));
                Map<Long, Map<String, BeaconClusterRoute>> newOrgHostRouteMap = newOrgRoutes.stream()
                    .collect(Collectors.toMap(BeaconOrgRoute::getOrgId, or -> or.getClusterRoutes()
                        .stream().collect(Collectors.toMap(BeaconClusterRoute::getName, Function.identity()))));

                oldOrgHostRouteMap.forEach((orgId, oldHostRouteMap) -> {
                    Map<String, BeaconClusterRoute> newHostRouteMap = newOrgHostRouteMap.get(orgId);
                    if (MapUtils.isEmpty(newHostRouteMap)) {
                        // 组织粒度的监控集群拆分目前未支持，仅做log记录
                        logger.warn("delete org id - {}, this operation is not support now, please check config ", orgId);
                        return;
                    }
                    DefaultMonitorClusterManager monitorClusterManager = this.orgMonitorMap.get(orgId);
                    Map<String, MonitorService> nameServiceMap = monitorClusterManager.getServices().stream()
                        .collect(Collectors.toMap(MonitorService::getName, Function.identity()));

                    oldHostRouteMap.forEach((name, oldClusterRoute) -> {
                        MonitorService service = nameServiceMap.get(name);
                        BeaconClusterRoute newClusterRoute = newHostRouteMap.get(name);
                        if (newClusterRoute == null) {
                            monitorClusterManager.removeService(service);
                        } else if (!newClusterRoute.equals(oldClusterRoute)) {
                            monitorClusterManager.updateServiceWeight(service, newClusterRoute.getWeight());
                        }
                    });
                    newHostRouteMap.forEach((name, newClusterRoute) -> {
                        if (!oldHostRouteMap.containsKey(name)) {
                            monitorClusterManager.addService(this.constructMonitorService(newClusterRoute));
                        }
                    });
                });

                newOrgHostRouteMap.forEach((orgId, newHostRouteMap) -> {
                    if (!oldOrgHostRouteMap.containsKey(orgId)) {
                        // 组织粒度的监控集群拆分目前未支持，仅做log记录
                        logger.warn("add org id - {}, this operation is not support now, please check config", orgId);
                    }
                });
            }));
    }

    @Override
    public MonitorService get(long orgId, String clusterName) {
        DefaultMonitorClusterManager monitorClusterManager = orgMonitorMap.get(orgId);
        if (monitorClusterManager == null) {
            monitorClusterManager = orgMonitorMap.get(XPipeConsoleConstant.DEFAULT_ORG_ID);
            if (monitorClusterManager == null) {
                return null;
            }
        }
        return monitorClusterManager.getService(clusterName);
    }

    @Override
    public Map<Long, List<MonitorService>> getAllServices() {
        Map<Long, List<MonitorService>> map = new HashMap<>();
        this.orgMonitorMap.forEach((orgId, monitor) -> map.put(orgId, monitor.getServices()));
        return map;
    }

    private MonitorService constructMonitorService(BeaconClusterRoute route) {
        return MonitorServiceFactory.DEFAULT.build(route.getName(), route.getHost(), route.getWeight());
    }

}
