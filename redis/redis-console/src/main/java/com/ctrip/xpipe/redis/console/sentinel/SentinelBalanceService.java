package com.ctrip.xpipe.redis.console.sentinel;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.model.SentinelGroupModel;

import java.util.List;
import java.util.Map;

/**
 * @author lishanglin
 * date 2021/8/31
 */
public interface SentinelBalanceService {

    List<SentinelGroupModel> getCachedDcSentinel(String dcId, ClusterType clusterType, String tag);

    SentinelGroupModel selectSentinel(String dcId, ClusterType clusterType, String tag);

    SentinelGroupModel selectSentinelWithoutCache(String dcId, ClusterType clusterType, String tag);

    Map<Long, SentinelGroupModel> selectMultiDcSentinels(ClusterType clusterType, String tag);

    void rebalanceDcSentinel(String dc, ClusterType clusterType, String tag);

    void rebalanceBackupDcSentinel(String dc, String tag);

    void cancelCurrentBalance(String dc, ClusterType clusterType, String tag);

    SentinelBalanceTask getBalanceTask(String dc, ClusterType clusterType, String tag);

}
