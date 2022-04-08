package com.ctrip.xpipe.api.migration.auto;

import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;

import java.util.Set;

/**
 * @author lishanglin
 * date 2021/1/10
 */
public interface MonitorService {

    String getHost();

    Set<String> fetchAllClusters(String system);

    void registerCluster(String system, String clusterName, Set<MonitorGroupMeta> groups);

    void unregisterCluster(String system, String clusterName);

}
