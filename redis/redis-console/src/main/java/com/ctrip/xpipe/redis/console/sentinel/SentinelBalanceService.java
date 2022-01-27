package com.ctrip.xpipe.redis.console.sentinel;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.model.SentinelGroupInfo;
import com.ctrip.xpipe.redis.console.model.SetinelTbl;

import java.util.List;
import java.util.Map;

/**
 * @author lishanglin
 * date 2021/8/31
 */
public interface SentinelBalanceService {

    List<SetinelTbl> getCachedDcSentinel(String dcId);

    SetinelTbl selectSentinel(String dcId);

    SentinelGroupInfo selectSentinelByDcAndType(String dcId, ClusterType clusterType);

    SetinelTbl selectSentinelWithoutCache(String dcId);

    Map<Long, SetinelTbl> selectMultiDcSentinels();

    Map<Long, SentinelGroupInfo> selectMultiDcSentinelsByType(ClusterType clusterType);

    void rebalanceDcSentinel(String dc);

    void rebalanceBackupDcSentinel(String dc);

    void cancelCurrentBalance(String dc);

    SentinelBalanceTask getBalanceTask(String dc);

    void bindShardAndSentinelsByType(ClusterType clusterType);
}
