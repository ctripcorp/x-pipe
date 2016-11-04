package com.ctrip.xpipe.redis.console.service;

import java.util.List;

import com.ctrip.xpipe.redis.console.model.SetinelTbl;

public interface SentinelService {
	List<SetinelTbl> findAllByDcName(String dcName);
}
