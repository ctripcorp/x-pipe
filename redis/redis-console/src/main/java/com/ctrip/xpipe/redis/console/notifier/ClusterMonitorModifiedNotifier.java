package com.ctrip.xpipe.redis.console.notifier;

/**
 * @author lishanglin
 * date 2021/1/18
 */
public interface ClusterMonitorModifiedNotifier {

    void notifyClusterUpdate(final String clusterName, long orgId);

    void notifyClusterDelete(final String clusterName, long orgId);

}
