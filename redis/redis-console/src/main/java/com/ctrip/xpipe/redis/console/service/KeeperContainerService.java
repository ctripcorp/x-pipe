package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.controller.api.data.meta.KeeperContainerCreateInfo;
import com.ctrip.xpipe.redis.console.model.KeeperContainerInfoModel;
import com.ctrip.xpipe.redis.console.model.KeeperMsgModel;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface KeeperContainerService {

	KeepercontainerTbl find(long id);
	KeepercontainerTbl find(String ip);

	List<KeepercontainerTbl> findAll();
	List<KeepercontainerTbl> findAllByDcName(String dcName);
	List<KeepercontainerTbl> findAllActiveByDcName(String dcName);
	List<KeepercontainerTbl> findKeeperCount(String dcName);
	/**
	 * Candidate keeper containers for a dc/cluster: pool (BU/org) → tag (with degrade) → count ascending.
	 * Does <b>not</b> filter by disk type or diversify by AZ; callers apply those as needed.
	 */
	List<KeepercontainerTbl> findBestKeeperContainersByDcCluster(String dcName, String clusterName);
	/**
	 * Multi-AZ diversify: keep at most one keeper container per active AZ (input order preserved for first-wins).
	 * When the DC has ≤1 active AZ, returns the input list unchanged.
	 */
	List<KeepercontainerTbl> filterKeeperContainersByAz(List<KeepercontainerTbl> keeperContainers, String dcName);
	List<KeepercontainerTbl> getKeeperContainerByAz(Long azId);

	List<Set<Long>> divideKeeperContainers(int partsCount);

	List<KeeperContainerInfoModel> findAllInfos();
	KeeperContainerInfoModel findKeeperContainerInfoModelById(long id);
	List<KeeperContainerInfoModel> findAvailableKeeperContainerInfoModelsByDcAzOrgAndTag(String dcName, String azName, String orgName, String tag);

	void addKeeperContainer(KeeperContainerCreateInfo createInfo);

	List<KeeperContainerCreateInfo> getDcAllKeeperContainers(String dcName);

	void updateKeeperContainer(KeeperContainerCreateInfo createInfo);

	void deleteKeeperContainer(String keepercontainerIp, int keepercontainerPort);

	void addKeeperContainerByInfoModel(KeeperContainerInfoModel keeperContainerInfoModel);

	void updateKeeperContainerByInfoModel(KeeperContainerInfoModel keeperContainerInfoModel);

	Map<Long, Long> keeperContainerIdDcMap();

	List<KeeperMsgModel> getAllKeepers(String keeperIp);
}
