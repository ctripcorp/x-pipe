package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterModel;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.consoleportal.ClusterListUnhealthyClusterModel;

import java.util.List;

public interface ClusterService {

	ClusterTbl find(String clusterName);
	List<ClusterTbl> findAllByNames(List<String> clusterNames);
	ClusterTbl findClusterAndOrg(String clusterName);
	ClusterStatus clusterStatus(String clusterName);
	List<DcTbl> getClusterRelatedDcs(String clusterName);

	ClusterTbl find(long clusterId);
	List<ClusterTbl> findAllClustersWithOrgInfo();
	List<ClusterTbl> findClustersWithOrgInfoByClusterType(String clusterType);
	List<ClusterTbl> findClustersWithOrgInfoByActiveDcId(long activeDc);
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
	List<String> reBalanceSentinels(String dcName, int numOfClusters, boolean activeOnly);
	void reBalanceClusterSentinels(String dcName, List<String> clusterNames);

	List<ClusterListUnhealthyClusterModel> findUnhealthyClusters();
	List<ClusterTbl> findAllClusterByDcNameBind(String dcName);
	List<ClusterTbl> findActiveClustersByDcName(String dcName);
	List<ClusterTbl> findAllClustersByDcName(String dcName);

	List<ClusterTbl> findAllClusterByKeeperContainer(long keeperContainerId);

}
