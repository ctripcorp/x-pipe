package com.ctrip.xpipe.redis.console.sentinel;

import com.ctrip.xpipe.cluster.SentinelType;
import com.ctrip.xpipe.redis.console.model.SentinelGroupModel;

import java.util.List;
import java.util.Map;

/**
 * @author lishanglin
 * date 2021/8/31
 */
public interface SentinelBalanceService {

    List<SentinelGroupModel> getCachedDcSentinel(String dcId, SentinelType sentinelType);

    SentinelGroupModel selectSentinel(String dcId, SentinelType sentinelType);

    SentinelGroupModel selectSentinelWithoutCache(String dcId, SentinelType sentinelType);

    Map<Long, SentinelGroupModel> selectMultiDcSentinels(SentinelType sentinelType);

    void rebalanceDcSentinel(String dc, SentinelType sentinelType);

    void rebalanceBackupDcSentinel(String dc);

    void cancelCurrentBalance(String dc, SentinelType sentinelType);

    SentinelBalanceTask getBalanceTask(String dc, SentinelType sentinelType);

    void bindShardAndSentinelsByType(SentinelType clusterType);
}
