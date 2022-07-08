package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.DcModel;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.consoleportal.DcListDcModel;

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
	DcTbl findByDcName(String activeDcName);
	Map<Long, String> dcNameMap();
    Map<String, Long> dcNameIdMap();
    Map<String, Long> dcNameZoneMap();
	List<DcListDcModel> findAllDcsRichInfo();

	void insertWithPartField(long zoneId, String dcName, String description);

	DcModel findDcModelByDcName(String dcName);

	DcModel findDcModelByDcId(long dcId);
}
