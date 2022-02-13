package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.SentinelTbl;

import java.util.List;

public interface SentinelService {
	List<SentinelTbl> findAll();

	List<SentinelTbl> findAllWithDcName();

	List<SentinelTbl> findAllByDcName(String dcName);

	List<SentinelTbl> findBySentinelGroupId(long sentinelGroupId);

	List<SentinelTbl> findBySentinelGroupIdDeleted(long sentinelGroupId);

	SentinelTbl findByIpPort(String ip,int port);

//	SentinelGroupModel findById(long sentinelGroupId);

	SentinelTbl insert(SentinelTbl sentinelTbl);

	void updateByPk(SentinelTbl sentinelTbl);

//	SentinelModel updateSentinelTblAddr(SentinelModel sentinel);

	void delete(long id);

	void reheal(long id);
}
