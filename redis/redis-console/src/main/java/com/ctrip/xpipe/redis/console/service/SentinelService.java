package com.ctrip.xpipe.redis.console.service;

import java.util.List;
import java.util.Map;

import com.ctrip.xpipe.redis.console.model.SetinelTbl;

public interface SentinelService {

	List<SetinelTbl> findAllByDcName(String dcName);

	Map<Long, List<SetinelTbl>>   allSentinelsByDc();

	Map<Long, SetinelTbl>   eachRandomSentinelByDc();

	SetinelTbl find(long id);

	Map<Long, SetinelTbl> findByShard(long shardId);

	SetinelTbl insert(SetinelTbl setinelTbl);
}
