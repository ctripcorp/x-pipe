package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cache.TimeBoundCache;
import com.ctrip.xpipe.redis.checker.spring.ConsoleDisableDbCondition;
import com.ctrip.xpipe.redis.checker.spring.DisableDbMode;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.dto.ClusterDTO;
import com.ctrip.xpipe.redis.console.dto.ClusterUpdateDTO;
import com.ctrip.xpipe.redis.console.dto.MultiGroupClusterCreateDTO;
import com.ctrip.xpipe.redis.console.dto.SingleGroupClusterCreateDTO;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.model.consoleportal.ClusterListUnhealthyClusterModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.RouteInfoModel;
import com.ctrip.xpipe.redis.console.resources.ConsolePortalService;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.stream.IntStream;

@Service
@Conditional(ConsoleDisableDbCondition.class)
@DisableDbMode(true)
public class ClusterServiceWithoutDB implements ClusterService {

    @Autowired
    private DcService dcService;

    @Autowired
    private ConsolePortalService consolePortalService;

    private TimeBoundCache<List<ClusterTbl>> allClusters;

    @Autowired
    private ConsoleConfig config;

    @PostConstruct
    public void init() {
        allClusters = new TimeBoundCache<>(config::getCacheRefreshInterval, consolePortalService::findAllClusters);
    }

    @Override
    public ClusterTbl find(String clusterName) {
        List<ClusterTbl> all = allClusters.getData();
        for(ClusterTbl clusterTbl : all) {
            if(StringUtil.trimEquals(clusterTbl.getClusterName(), clusterName)) {
                return clusterTbl;
            }
        }
        return null;
    }

    @Override
    public List<ClusterTbl> findClustersByGroupType(String groupType) {
        List<ClusterTbl> all = allClusters.getData();
        List<ClusterTbl> result = new ArrayList<>();
        for(ClusterTbl clusterTbl : all) {
            if(StringUtil.trimEquals(clusterTbl.getGroupType(), groupType)) {
                result.add(clusterTbl);
            }
        }

        return result;
    }

    @Override
    public List<ClusterTbl> findAllByNames(List<String> clusterNames) {
        List<ClusterTbl> all = allClusters.getData();
        Set<String> names = new HashSet<>(clusterNames);
        List<ClusterTbl> result = new ArrayList<>();
        for(ClusterTbl clusterTbl : all) {
            if(names.contains(clusterTbl.getClusterName())) {
                result.add(clusterTbl);
            }
        }
        return result;
    }

    @Override
    public ClusterTbl findClusterAndOrg(String clusterName) {
        return find(clusterName);
    }

    @Override
    public ClusterStatus clusterStatus(String clusterName) {
        ClusterTbl clusterTbl = find(clusterName);
        if(clusterTbl != null) {
            return Enum.valueOf(ClusterStatus.class, clusterTbl.getStatus());
        }
        return null;
    }

    @Override
    public List<DcTbl> getClusterRelatedDcs(String clusterName) {
        ClusterTbl clusterTbl = find(clusterName);
        if(clusterTbl != null) {
            return clusterTbl.getDcInfo();
        }
        return Collections.emptyList();
    }

    @Override
    public boolean containsRedisInstance(String clusterName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClusterTbl find(long clusterId) {
        List<ClusterTbl> all = allClusters.getData();
        for(ClusterTbl clusterTbl : all) {
            if(clusterTbl.getId() == clusterId) {
                return clusterTbl;
            }
        }
        return null;
    }

    @Override
    public List<ClusterTbl> findAllClustersWithOrgInfo() {
        return allClusters.getData();
    }

    @Override
    public List<ClusterTbl> findClustersWithOrgInfoByClusterType(String clusterType) {
        List<ClusterTbl> all = allClusters.getData();
        List<ClusterTbl> result = new ArrayList<>();
        for(ClusterTbl clusterTbl : all) {
            if(StringUtil.trimEquals(clusterTbl.getClusterType(), clusterType)) {
                result.add(clusterTbl);
            }
        }

        return result;
    }

    @Override
    public List<ClusterTbl> findClustersWithOrgInfoByActiveDcId(long activeDc) {
        List<ClusterTbl> all = allClusters.getData();
        List<ClusterTbl> result = new ArrayList<>();
        for(ClusterTbl clusterTbl : all) {
            if(clusterTbl.getDcId() == activeDc) {
                result.add(clusterTbl);
            }
        }

        return result;
    }

    @Override
    public List<String> findAllClusterNames() {
        List<ClusterTbl> all = allClusters.getData();
        List<String> result = new ArrayList<>();
        for(ClusterTbl clusterTbl : all) {
            result.add(clusterTbl.getClusterName());
        }
        return result;
    }

    @Override
    public String findClusterTag(String clusterName) {
        ClusterTbl clusterTbl =  find(clusterName);
        if(clusterTbl != null) {
            return clusterTbl.getTag();
        }
        return null;
    }

    @Override
    public void updateClusterTag(String clusterName, String clusterTag) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long getAllCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClusterDTO getCluster(String clusterName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<ClusterDTO> getClusters(String clusterType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<ClusterDTO> getClusterWithShards(String clusterType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClusterTbl createCluster(ClusterModel clusterModel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createSingleGroupCluster(SingleGroupClusterCreateDTO clusterCreateDTO) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createMultiGroupCluster(MultiGroupClusterCreateDTO clusterCreateDTO) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String updateCluster(ClusterUpdateDTO clusterUpdateDTO) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateCluster(String clusterName, ClusterModel cluster) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateActivedcId(long id, long activeDcId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateStatusById(long id, ClusterStatus clusterStatus, long migrationEventId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteCluster(String clusterName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void bindDc(DcClusterTbl dcClusterTbl) {
        consolePortalService.bindDc(dcClusterTbl);
    }

    @Override
    public void bindDc(String clusterName, String dcName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unbindAz(String clusterName, String azName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unbindDc(String clusterName, String dcName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void update(ClusterTbl cluster) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void exchangeName(Long formerClusterId, String formerClusterName, Long latterClusterId, String latterClusterName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void exchangeRegion(Long formerClusterId, String formerClusterName, Long latterClusterId, String latterClusterName, String regionName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void upgradeAzGroup(String clusterName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void downgradeAzGroup(String clusterName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void bindRegionAz(String clusterName, String regionName, String azName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> findMigratingClusterNames() {
        return Collections.emptySet();
    }

    @Override
    public List<ClusterTbl> findErrorMigratingClusters() {
        return Collections.emptyList();
    }

    @Override
    public List<ClusterTbl> findMigratingClusters() {
        return Collections.emptyList();
    }

    @Override
    public List<ClusterListUnhealthyClusterModel> findUnhealthyClusters() {
        return Collections.emptyList();
    }

    @Override
    public List<ClusterTbl> findAllClusterByDcNameBind(String dcName) {
        return Collections.emptyList();
    }

    @Override
    public List<ClusterTbl> findAllClusterByDcNameBindAndType(String dcName, String clusterType, boolean isCountTypeInHetero) {
        return Collections.emptyList();
    }

    @Override
    public List<ClusterTbl> findActiveClustersByDcName(String dcName) {
        return Collections.emptyList();
    }

    @Override
    public List<ClusterTbl> findActiveClustersByDcNameAndType(String dcName, String clusterType, boolean isCountTypeInHetero) {
        return Collections.emptyList();
    }

    @Override
    public List<ClusterTbl> findAllClustersByDcName(String dcName) {
        return Collections.emptyList();
    }

    @Override
    public List<ClusterTbl> findAllClusterByKeeperContainer(long keeperContainerId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Set<String>> divideClusters(int partsCnt) {
        List<ClusterTbl> allClusters = findAllClustersWithOrgInfo();
        if (null == allClusters) return Collections.emptyList();
        List<Set<String>> parts = new ArrayList<>(partsCnt);
        IntStream.range(0, partsCnt).forEach(i -> parts.add(new HashSet<>()));

        allClusters.forEach(clusterTbl -> parts.get((int) (clusterTbl.getId() % partsCnt)).add(clusterTbl.getClusterName()));
        return parts;
    }

    @Override
    public List<RouteInfoModel> findClusterDefaultRoutesBySrcDcNameAndClusterName(String backupDcId, String clusterName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RouteInfoModel> findClusterUsedRoutesBySrcDcNameAndClusterName(String backupDcId, String clusterName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RouteInfoModel> findClusterDesignateRoutesBySrcDcNameAndClusterName(String dcName, String clusterName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateClusterDesignateRoutes(String clusterName, String srcDcName, List<RouteInfoModel> newDesignatedRoutes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UnexpectedRouteUsageInfoModel findUnexpectedRouteUsageInfoModel() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void completeReplicationByClusterAndReplDirection(ClusterTbl cluster, ReplDirectionInfoModel replDirection) {
        throw new UnsupportedOperationException();
    }

}
