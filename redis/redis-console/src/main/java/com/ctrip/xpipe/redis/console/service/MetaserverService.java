package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.MetaserverTbl;

import java.util.List;

public interface MetaserverService {
	List<MetaserverTbl> findAllByDcName(String dcName);
}
