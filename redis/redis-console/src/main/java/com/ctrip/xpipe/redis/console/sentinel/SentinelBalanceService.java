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

    List<SentinelGroupModel> getCachedDcSentinel(String dcId, ClusterType clusterType);

    SentinelGroupModel selectSentinel(String dcId, ClusterType clusterType);

    SentinelGroupModel selectSentinelWithoutCache(String dcId, ClusterType clusterType);

    Map<Long, SentinelGroupModel> selectMultiDcSentinels(ClusterType clusterType);

    void rebalanceDcSentinel(String dc, ClusterType clusterType);

    void rebalanceBackupDcSentinel(String dc);

    void cancelCurrentBalance(String dc, ClusterType clusterType);

    SentinelBalanceTask getBalanceTask(String dc, ClusterType clusterType);

}
