package com.ctrip.xpipe.redis.console.service;

import java.util.List;
import java.util.Map;

import com.ctrip.xpipe.redis.console.model.SetinelTbl;

public interface SentinelService {
	List<SetinelTbl> findAllByDcName(String dcName);
	SetinelTbl find(long id);
	Map<Long, SetinelTbl> findByShard(long shardId);
}
