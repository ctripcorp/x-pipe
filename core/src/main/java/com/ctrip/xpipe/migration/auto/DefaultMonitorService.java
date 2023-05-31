package com.ctrip.xpipe.migration.auto;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;

import java.util.Collections;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/1/26
 */
public class DefaultMonitorService implements MonitorService {

    private String name;
    private String host;
    private int weight;

    public DefaultMonitorService(String name, String host, int weight) {
        this.name = name;
        this.host = host;
        this.weight = weight;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public int getWeight() {
        return weight;
    }

    @Override
    public void setWeight(int weight) {
        this.weight = weight;
    }

    @Override
    public Set<String> fetchAllClusters(String system) {
        return Collections.emptySet();
    }

    @Override
    public void registerCluster(String system, String clusterName, Set<MonitorGroupMeta> groups) {
        // do nothing
    }

    @Override
    public void unregisterCluster(String system, String clusterName) {
        // do nothing
    }

}
