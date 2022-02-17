package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.model.SentinelGroupModel;
import com.ctrip.xpipe.redis.console.model.SentinelUsageModel;

import java.util.List;
import java.util.Map;

public interface SentinelGroupService {

    List<SentinelGroupModel> findAllByDcName(String dcName);

    List<SentinelGroupModel> findAllByDcAndType(String dcName, ClusterType clusterType);

    SentinelGroupModel findById(long sentinelGroupId);

    Map<Long, SentinelGroupModel> findByShard(long shardId);

    void addSentinelGroup(SentinelGroupModel sentinelGroupModel);

    List<SentinelGroupModel> getSentinelGroupsWithUsageByType(ClusterType clusterType);

    List<SentinelGroupModel> getAllSentinelGroupsWithUsage();

    Map<String, SentinelUsageModel> getAllSentinelsUsage();

    void updateSentinelGroupAddress(SentinelGroupModel sentinelGroupModel);

    RetMessage removeSentinelMonitor(String clusterName);

    void delete(long id);

    void reheal(long id);

}
