package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.model.consoleportal.ClusterListUnhealthyClusterModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.RouteInfoModel;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ClusterService {

	ClusterTbl find(String clusterName);
    List<ClusterTbl> findClustersByGroupType(String groupType);
    List<ClusterTbl> findAllByNames(List<String> clusterNames);
	ClusterTbl findClusterAndOrg(String clusterName);
	ClusterStatus clusterStatus(String clusterName);
	List<DcTbl> getClusterRelatedDcs(String clusterName);

	ClusterTbl find(long clusterId);
	List<ClusterTbl> findAllClustersWithOrgInfo();
	List<ClusterTbl> findClustersWithOrgInfoByClusterType(String clusterType);
	List<ClusterTbl> findClustersWithOrgInfoByActiveDcId(long activeDc);
	List<String> findAllClusterNames();
	Long getCountByActiveDc(long activeDcId);
	Map<String, Long> getAllCountByActiveDc();
	Map<String, Long> getMigratableClustersCountByActiveDc();
    Long getCountByActiveDcAndClusterType(long activeDc, String clusterType);
    Long getAllCount();
	ClusterTbl createCluster(ClusterModel clusterModel);
	void updateCluster(String clusterName, ClusterModel cluster);

	void updateActivedcId(long id, long activeDcId);
	void updateStatusById(long id, ClusterStatus clusterStatus, long migrationEventId);
	void deleteCluster(String clusterName);
	void bindDc(DcClusterTbl dcClusterTbl);
	void unbindDc(String clusterName, String dcName);
	void update(ClusterTbl cluster);
	void exchangeName(Long formerClusterId, String formerClusterName, Long latterClusterId, String latterClusterName);

	Set<String> findMigratingClusterNames();
	List<ClusterTbl> findErrorMigratingClusters();
	List<ClusterTbl> findMigratingClusters();
	default void resetClustersStatus(List<Long> ids) {
		for (Long id : ids) {
			updateStatusById(id, ClusterStatus.Normal, 0L);
		}
	}

	List<ClusterListUnhealthyClusterModel> findUnhealthyClusters();
	List<ClusterTbl> findAllClusterByDcNameBind(String dcName);
	List<ClusterTbl> findAllClusterByDcNameBindAndType(String dcName, String clusterType);
	List<ClusterTbl> findActiveClustersByDcName(String dcName);
	List<ClusterTbl> findActiveClustersByDcNameAndType(String dcName, String clusterType);
	List<ClusterTbl> findAllClustersByDcName(String dcName);

	List<ClusterTbl> findAllClusterByKeeperContainer(long keeperContainerId);

	List<Set<String>> divideClusters(int parts);

	List<RouteInfoModel> findClusterDefaultRoutesBySrcDcNameAndClusterName(String backupDcId, String clusterName);
	List<RouteInfoModel> findClusterUsedRoutesBySrcDcNameAndClusterName(String backupDcId, String clusterName);
	List<RouteInfoModel> findClusterDesignateRoutesBySrcDcNameAndClusterName(String dcName, String clusterName);
	void updateClusterDesignateRoutes(String clusterName, String srcDcName, List<RouteInfoModel> newDesignatedRoutes);
	UnexpectedRouteUsageInfoModel findUnexpectedRouteUsageInfoModel();

    void completeReplicationByClusterAndReplDirection(ClusterTbl cluster, ReplDirectionInfoModel replDirection);
}
