package com.ctrip.xpipe.redis.console.service;

import java.util.List;

import com.ctrip.xpipe.redis.console.model.DcClusterTbl;

public interface DcClusterService {

	DcClusterTbl find(long dcId, long clusterId);
	DcClusterTbl find(String dcName, String clusterName);
	List<DcClusterTbl> findAllDcClusters();
	DcClusterTbl addDcCluster(String dcName, String clusterName);
	List<DcClusterTbl> findByClusterIds(List<Long> clusterIds);
}
