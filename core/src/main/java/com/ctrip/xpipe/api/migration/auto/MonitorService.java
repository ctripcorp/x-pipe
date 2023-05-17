package com.ctrip.xpipe.api.migration.auto;

import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;

import java.beans.PropertyChangeListener;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/1/10
 */
public interface MonitorService {

    String REGISTER = "register";
    String UNREGISTER = "unregister";

    int MIN_WEIGHT = 0;
    int MAX_WEIGHT = 100;
    String SEPARATOR = "#@#";

    String getName();

    String getHost();

    int getWeight();

    void setWeight(int weight);

    Set<String> fetchAllClusters(String system);

    void registerCluster(String system, String clusterName, Set<MonitorGroupMeta> groups);

    void unregisterCluster(String system, String clusterName);

    void addChangeListener(PropertyChangeListener listener);

    void removeAllChangeListener();

}
