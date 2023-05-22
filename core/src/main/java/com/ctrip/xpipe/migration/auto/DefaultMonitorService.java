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

    private String host;

    public DefaultMonitorService(String host) {
        this.host = host;
    }

    @Override
    public String getHost() {
        return host;
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
    public void unregisterCluster(String systemm, String clusterName) {
        // do nothing
    }
}
