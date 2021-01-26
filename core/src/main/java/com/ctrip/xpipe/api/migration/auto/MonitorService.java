package com.ctrip.xpipe.api.migration.auto;

import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;

import java.util.Set;

/**
 * @author lishanglin
 * date 2021/1/10
 */
public interface MonitorService {

    String getHost();

    Set<String> fetchAllClusters();

    void registerCluster(String clusterName, Set<MonitorGroupMeta> groups);

    void unregisterCluster(String clusterName);

}
