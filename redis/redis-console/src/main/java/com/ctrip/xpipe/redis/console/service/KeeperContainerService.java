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

	KeepercontainerTbl findByHostName(String hostname);
	KeepercontainerTbl findByHostNameAndPort(String hostname, int port);

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


	List<KeeperContainerInfoModel> findAllInfos();
	KeeperContainerInfoModel findKeeperContainerInfoModelById(long id);
	List<KeeperContainerInfoModel> findAvailableKeeperContainerInfoModelsByDcAzOrgAndTag(String dcName, String azName, String orgName, String tag);

	void addKeeperContainer(KeeperContainerCreateInfo createInfo);

	List<KeeperContainerCreateInfo> getDcAllKeeperContainers(String dcName);

	void updateKeeperContainer(KeeperContainerCreateInfo createInfo);

	void deleteKeeperContainer(String keepercontainerIp, int keepercontainerPort);

	void deleteKeeperContainerByHostname(String keepercontainerHostName, int keepercontainerPort);

	void addKeeperContainerByInfoModel(KeeperContainerInfoModel keeperContainerInfoModel);

	void updateKeeperContainerByInfoModel(KeeperContainerInfoModel keeperContainerInfoModel);

	Map<Long, Long> keeperContainerIdDcMap();

	/**
	 * 查询指定 keepercontainer 上的 keeper 实例明细。
	 * 容器化后 ip 不再唯一标识容器（pod 重建会变、多实例可能共用占位 ip），因此按 keepercontainer_id 定位
	 */
	List<KeeperMsgModel> getAllKeepers(long keeperContainerId);
}
