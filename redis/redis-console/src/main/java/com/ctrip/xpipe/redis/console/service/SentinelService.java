package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.model.SentinelGroupInfo;
import com.ctrip.xpipe.redis.console.model.SentinelModel;
import com.ctrip.xpipe.redis.console.model.SentinelUsageModel;
import com.ctrip.xpipe.redis.console.model.SetinelTbl;

import java.util.List;
import java.util.Map;

public interface SentinelService {

	List<SetinelTbl> findAllByDcName(String dcName);

	List<SentinelGroupInfo> findAllByDcAndType(String dcName, ClusterType clusterType);

	SetinelTbl find(long id);

	SentinelGroupInfo findById(long sentinelGroupId);

	Map<Long, SetinelTbl> findByShard(long shardId);

	Map<Long, SentinelGroupInfo> findSentinelGroupsByShard(long shardId);

	SetinelTbl insert(SetinelTbl setinelTbl);

	SentinelGroupInfo addSentinelGroup(SentinelGroupInfo sentinelGroupInfo);

	List<SetinelTbl> getAllSentinelsWithUsage();

	List<SentinelGroupInfo> getSentinelGroupsWithUsageByType(ClusterType clusterType);

	List<SentinelGroupInfo> getAllSentinelGroupsWithUsage();

	Map<String, SentinelUsageModel> getAllSentinelsUsage();

	Map<String, SentinelUsageModel> getSentinelGroupsUsageByType(ClusterType clusterType);

	SentinelModel updateSentinelTblAddr(SentinelModel sentinel);

	SentinelGroupInfo updateSentinelGroup(SentinelGroupInfo sentinelGroupInfo);

	RetMessage removeSentinelMonitor(String clusterName);

	void delete(long id);

	void reheal(long id);
}
