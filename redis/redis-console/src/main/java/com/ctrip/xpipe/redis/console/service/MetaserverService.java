package com.ctrip.xpipe.redis.console.service;

import java.util.List;

import com.ctrip.xpipe.redis.console.model.MetaserverTbl;

public interface MetaserverService {
	List<MetaserverTbl> findAllByDcName(String dcName);
}
