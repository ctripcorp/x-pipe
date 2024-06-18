package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.controller.api.data.meta.KeeperContainerCreateInfo;
import com.ctrip.xpipe.redis.console.model.KeeperContainerInfoModel;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;

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
	List<KeepercontainerTbl> findBestKeeperContainersByDcCluster(String dcName, String clusterName);
	List<KeepercontainerTbl> findBestKeeperContainersByDcCluster(String dcName, String clusterName, boolean skipAzFilter);
	List<KeepercontainerTbl> getKeeperContainerByAz(Long azId);

	List<Set<Long>> divideKeeperContainers(int partsCount);

	List<KeeperContainerInfoModel> findAllInfos();
	KeeperContainerInfoModel findKeeperContainerInfoModelById(long id);
	List<KeeperContainerInfoModel> findAvailableKeeperContainerInfoModelsByDcAzAndOrg(String dcName, String azName, String orgName);

	void addKeeperContainer(KeeperContainerCreateInfo createInfo);

	List<KeeperContainerCreateInfo> getDcAllKeeperContainers(String dcName);

	void updateKeeperContainer(KeeperContainerCreateInfo createInfo);

	void deleteKeeperContainer(String keepercontainerIp, int keepercontainerPort);

	void addKeeperContainerByInfoModel(KeeperContainerInfoModel keeperContainerInfoModel);

	void updateKeeperContainerByInfoModel(KeeperContainerInfoModel keeperContainerInfoModel);

	List<KeeperInstanceMeta> getAllKeepers(String keeperContainerIp);

	void resetKeeper(String activeKeeperIp, Long replId);

	Map<Long, Long> keeperContainerIdDcMap();
}
