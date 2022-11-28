package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.controller.api.data.meta.KeeperContainerCreateInfo;
import com.ctrip.xpipe.redis.console.model.KeeperContainerInfoModel;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;

import java.util.List;
import java.util.Map;

public interface KeeperContainerService {

	KeepercontainerTbl find(long id);
	List<KeepercontainerTbl> findAll();
	List<KeepercontainerTbl> findAllByDcName(String dcName);
	List<KeepercontainerTbl> findAllActiveByDcName(String dcName);
	List<KeepercontainerTbl> findKeeperCount(String dcName);
	List<KeepercontainerTbl> findBestKeeperContainersByDcCluster(String dcName, String clusterName);
	List<KeepercontainerTbl> getKeeperContainerByAz(Long azId);

	List<KeeperContainerInfoModel> findAllInfos();
	KeeperContainerInfoModel findKeeperContainerInfoModelById(long id);
	List<KeeperContainerInfoModel> findAvailableKeeperContainerInfoModelsByDcAzAndOrg(String dcName, String azName, String orgName);

	void addKeeperContainer(KeeperContainerCreateInfo createInfo);

	List<KeeperContainerCreateInfo> getDcAllKeeperContainers(String dcName);

	void updateKeeperContainer(KeeperContainerCreateInfo createInfo);

	void deleteKeeperContainer(String keepercontainerIp, int keepercontainerPort);

	void addKeeperContainerByInfoModel(KeeperContainerInfoModel keeperContainerInfoModel);

	void updateKeeperContainerByInfoModel(KeeperContainerInfoModel keeperContainerInfoModel);

	Map<Long, Long> keeperContainerIdDcMap();
}
