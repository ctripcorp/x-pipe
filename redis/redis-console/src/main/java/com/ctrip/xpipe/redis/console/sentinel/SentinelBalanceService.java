package com.ctrip.xpipe.redis.console.sentinel;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.cluster.DcGroupType;
import com.ctrip.xpipe.redis.console.model.SentinelGroupModel;

import java.util.List;
import java.util.Map;

/**
 * @author lishanglin
 * date 2021/8/31
 */
public interface SentinelBalanceService {

    List<SentinelGroupModel> getCachedDcSentinel(String dcId, ClusterType clusterType, DcGroupType... dcGroupTypes);

    SentinelGroupModel selectSentinel(String dcId, ClusterType clusterType, DcGroupType... dcGroupTypes);

    SentinelGroupModel selectSentinelWithoutCache(String dcId, ClusterType clusterType, DcGroupType... dcGroupTypes);

    Map<Long, SentinelGroupModel> selectMultiDcSentinels(ClusterType clusterType, DcGroupType... dcGroupTypes);

    void rebalanceDcSentinel(String dc, ClusterType clusterType);

    void rebalanceBackupDcSentinel(String dc);

    void cancelCurrentBalance(String dc, ClusterType clusterType);

    SentinelBalanceTask getBalanceTask(String dc, ClusterType clusterType);

}
