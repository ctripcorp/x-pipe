package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.model.SentinelModel;
import com.ctrip.xpipe.redis.console.model.SentinelUsageModel;
import com.ctrip.xpipe.redis.console.model.SetinelTbl;

import java.util.List;
import java.util.Map;

public interface SentinelService {

	List<SetinelTbl> findAllByDcName(String dcName);

	Map<Long, List<SetinelTbl>>   allSentinelsByDc();

	SetinelTbl find(long id);

	Map<Long, SetinelTbl> findByShard(long shardId);

	SetinelTbl insert(SetinelTbl setinelTbl);

	List<SetinelTbl> getAllSentinelsWithUsage();

	Map<String, SentinelUsageModel> getAllSentinelsUsage();

	SentinelModel updateSentinelTblAddr(SentinelModel sentinel);

	RetMessage removeSentinelMonitor(String clusterName);

	void delete(long id);

	void reheal(long id);
}
