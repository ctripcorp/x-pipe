package com.ctrip.xpipe.redis.console.notifier;

/**
 * Internal handler for a single Beacon route type (DR / SENTINEL).
 */
public interface BeaconRouteClusterMonitorNotifier {

    boolean needNotify(String clusterName, String dc, long orgId);

    void notifyClusterUpdate(String clusterName, String dc, long orgId, String lastModifyTime);

    void notifyClusterDelete(String clusterName, String dc, long orgId);

}
