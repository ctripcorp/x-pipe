package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.controller.api.data.meta.KeeperContainerCreateInfo;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;

import java.util.List;

public interface KeepercontainerService {

	KeepercontainerTbl find(long id);
	List<KeepercontainerTbl> findAllByDcName(String dcName);
	List<KeepercontainerTbl> findAllActiveByDcName(String dcName);
	List<KeepercontainerTbl> findKeeperCount(String dcName);
	List<KeepercontainerTbl> findBestKeeperContainersByDcCluster(String dcName, String clusterName);

	void addKeeperContainer(KeeperContainerCreateInfo createInfo);

	List<KeeperContainerCreateInfo> getDcAllKeeperContainers(String dcName);

	void updateKeeperContainer(KeeperContainerCreateInfo createInfo);
}
