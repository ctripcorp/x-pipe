package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.DcTbl;

import java.util.List;
import java.util.Map;

public interface DcService {
	DcTbl find(String dcName);
	DcTbl find(long dcId);
	String getDcName(long dcId);
	List<DcTbl> findAllDcs();
	List<DcTbl> findAllDcNames();
	List<DcTbl> findAllDcBasic();
	List<DcTbl> findClusterRelatedDc(String clusterName);
	List<DcTbl> findAllDetails(String dcName);
	List<DcTbl> findAllActiveKeepers();
	DcTbl findByDcName(String activeDcName);
	Map<Long, String> dcNameMap();
	
}
