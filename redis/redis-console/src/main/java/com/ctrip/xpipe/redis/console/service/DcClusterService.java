package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.DcClusterTbl;

import java.util.List;

public interface DcClusterService {

	DcClusterTbl find(long dcId, long clusterId);
	DcClusterTbl find(String dcName, String clusterName);
	List<DcClusterTbl> findAllDcClusters();
	DcClusterTbl addDcCluster(String dcName, String clusterName);
	List<DcClusterTbl> findByClusterIds(List<Long> clusterIds);
	List<DcClusterTbl> findAllByDcId(long dcId);
}
