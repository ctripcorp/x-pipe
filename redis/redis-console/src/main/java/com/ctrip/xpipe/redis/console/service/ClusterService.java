package com.ctrip.xpipe.redis.console.service;

import java.util.List;

import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterModel;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;

public interface ClusterService {

	ClusterTbl find(String clusterName);
	ClusterTbl find(long clusterId);
	List<ClusterTbl> findAllClusters();
	List<ClusterTbl> findClustersByActiveDcId(long activeDc);
	List<String> findAllClusterNames();
	Long getAllCount();
	ClusterTbl createCluster(ClusterModel clusterModel);
	void updateCluster(String clusterName, ClusterTbl cluster);

	void updateActivedcId(long id, long activeDcId);
	void updateStatusById(long id, ClusterStatus clusterStatus);
	void deleteCluster(String clusterName);
	void bindDc(String clusterName, String dcName);
	void unbindDc(String clusterName, String dcName);
	void update(ClusterTbl cluster);
}
