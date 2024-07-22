package com.ctrip.xpipe.redis.console.healthcheck.fulllink.metaserver.impl;

import com.ctrip.xpipe.cache.TimeBoundCache;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.metaserver.ConsoleMetaServerApiService;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.metaserver.MetaServerManager;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.metaserver.model.ClusterDebugInfo;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.metaserver.model.ClusterServerInfo;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.metaserver.model.SlotInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

@Component
public class DefaultMetaServerManager implements MetaServerManager {

    @Autowired
    private ConsoleMetaServerApiService metaServerApiService;

    @Autowired
    private ConsoleConfig consoleConfig;

    private TimeBoundCache<ClusterDebugInfo> dataCache;

    private TimeBoundCache<Map<Integer, HostPort>> metaServerIdIpMapCache;

    private TimeBoundCache<Map<Integer, Integer>> clusterIdMetaServerIdMapCache;


    @PostConstruct
    public void postConstruct(){
        dataCache = new TimeBoundCache<>(consoleConfig::getMetaServerSlotClusterMapCacheTimeOutMilli, this::refreshClusterDataCache);
        metaServerIdIpMapCache = new TimeBoundCache<>(consoleConfig::getMetaServerSlotClusterMapCacheTimeOutMilli, this::refreshMetaServerIdIpMap);
        clusterIdMetaServerIdMapCache = new TimeBoundCache<>(consoleConfig::getMetaServerSlotClusterMapCacheTimeOutMilli, this::refreshClusterIdMetaServerIdMap);
    }

    @Override
    public HostPort getLocalDcManagerMetaServer(long clusterId) {
        Integer slotId = Math.toIntExact(clusterId % 256);
        Integer metaServerId = clusterIdMetaServerIdMapCache.getData().get(slotId);
        return metaServerIdIpMapCache.getData().get(metaServerId);
    }

    private ClusterDebugInfo refreshClusterDataCache() {
        return metaServerApiService.getAllClusterSlotStateInfos();
    }

    private Map<Integer, HostPort> refreshMetaServerIdIpMap() {
        Map<Integer, ClusterServerInfo> integerClusterServerInfoMap = dataCache.getData().getClusterServerInfos();
        Map<Integer, HostPort> map = new HashMap<>();
        for (Map.Entry<Integer, ClusterServerInfo> entry : integerClusterServerInfoMap.entrySet()) {
            map.put(entry.getKey(), new HostPort(entry.getValue().getIp(), entry.getValue().getPort()));
        }
        return map;
    }

    private Map<Integer, Integer> refreshClusterIdMetaServerIdMap() {
        Map<Integer, SlotInfo> integerSlotInfoMap = dataCache.getData().getAllSlotInfo();
        Map<Integer, Integer> map = new HashMap<>();
        for (Map.Entry<Integer, SlotInfo> entry : integerSlotInfoMap.entrySet()) {
            map.put(entry.getKey(), entry.getValue().getServerId());
        }
        return map;
    }
}
