package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cache.TimeBoundCache;
import com.ctrip.xpipe.cluster.DcGroupType;
import com.ctrip.xpipe.redis.checker.spring.ConsoleDisableDbCondition;
import com.ctrip.xpipe.redis.checker.spring.DisableDbMode;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.DcClusterCreateInfo;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterModel;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.resources.ConsolePortalService;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Conditional(ConsoleDisableDbCondition.class)
@DisableDbMode(true)
public class DcClusterServiceWithoutDB implements DcClusterService {

    @Autowired
    private ConsolePortalService consolePortalService;

    @Autowired
    private ConsoleConfig config;

    @Autowired
    private DcService dcService;

    @Autowired
    private ClusterService clusterService;

    private TimeBoundCache<List<DcClusterTbl>> allDcClusters;

    @PostConstruct
    public void init() {
        allDcClusters = new TimeBoundCache<>(config::getCacheRefreshInterval, this::loadAllDcClusters);
    }

    private List<DcClusterTbl> loadAllDcClusters() {
        List<ClusterTbl> clusters = consolePortalService.findAllClusters();
        if (clusters == null) {
            return Collections.emptyList();
        }
        List<DcClusterTbl> result = new ArrayList<>();
        for (ClusterTbl cluster : clusters) {
            List<DcClusterCreateInfo> dcClusterInfos = consolePortalService.getDcClusterInfoOfCluster(cluster.getClusterName());
            if (dcClusterInfos == null) {
                continue;
            }
            for (DcClusterCreateInfo info : dcClusterInfos) {
                result.add(toDcClusterTbl(info, cluster));
            }
        }
        return result;
    }

    private DcClusterTbl toDcClusterTbl(DcClusterCreateInfo info, ClusterTbl cluster) {
        DcClusterTbl tbl = new DcClusterTbl();
        tbl.setDcName(info.getDcName());
        tbl.setClusterName(info.getClusterName());
        tbl.setActiveRedisCheckRules(info.getRedisCheckRule());
        DcTbl dc = dcService.find(info.getDcName());
        if (dc != null) {
            tbl.setDcId(dc.getId());
        }
        if (cluster != null) {
            tbl.setClusterId(cluster.getId());
        }
        return tbl;
    }

    @Override
    public DcClusterTbl find(long dcId, long clusterId) {
        for (DcClusterTbl dcCluster : allDcClusters.getData()) {
            if (dcCluster.getDcId() == dcId && dcCluster.getClusterId() == clusterId) {
                return dcCluster;
            }
        }
        return null;
    }

    @Override
    public DcClusterTbl find(String dcName, String clusterName) {
        for (DcClusterTbl dcCluster : allDcClusters.getData()) {
            if (StringUtil.trimEquals(dcCluster.getDcName(), dcName)
                    && StringUtil.trimEquals(dcCluster.getClusterName(), clusterName)) {
                return dcCluster;
            }
        }
        return null;
    }

    @Override
    public DcClusterTbl findByPK(long keyDcClusterId) {
        for (DcClusterTbl dcCluster : allDcClusters.getData()) {
            if (dcCluster.getDcClusterId() == keyDcClusterId) {
                return dcCluster;
            }
        }
        return null;
    }

    @Override
    public DcClusterCreateInfo findDcClusterCreateInfo(String dcName, String clusterName) {
        DcClusterTbl dcClusterTbl = find(dcName, clusterName);
        if (dcClusterTbl == null) {
            return null;
        }
        return new DcClusterCreateInfo()
                .setClusterName(clusterService.find(dcClusterTbl.getClusterId()).getClusterName())
                .setDcName(dcService.find(dcClusterTbl.getDcId()).getDcName())
                .setRedisCheckRule(dcClusterTbl.getActiveRedisCheckRules());
    }

    @Override
    public void updateDcCluster(DcClusterCreateInfo dcClusterCreateInfo) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<DcClusterTbl> findAllDcClusters() {
        return allDcClusters.getData();
    }

    @Override
    public DcClusterTbl addDcCluster(String dcName, String clusterName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<DcClusterModel> findRelatedDcClusterModels(long clusterId) {
        List<DcClusterTbl> dcClusterTbls = findClusterRelated(clusterId);
        List<DcClusterModel> result = new ArrayList<>();
        dcClusterTbls.forEach(dcClusterTbl -> result.add(new DcClusterModel().setDcCluster(dcClusterTbl)));
        return result;
    }

    @Override
    public void validateDcClusters(List<DcClusterModel> dcClusterModels, ClusterTbl clusterTbl) {
        dcClusterModels.forEach(dcClusterModel -> {
            if (clusterTbl.getId() != dcClusterModel.getDcCluster().getClusterId()) {
                throw new BadRequestException(String.format("dc cluster:{} should belong to cluster:{}, but belong to cluster:{}",
                        dcClusterModel.getDcCluster(), dcClusterModel.getDcCluster().getClusterId(), clusterTbl.getId()));
            }

            if (clusterTbl.getActivedcId() == dcClusterModel.getDcCluster().getDcId()
                    && DcGroupType.isSameGroupType(dcClusterModel.getDcCluster().getGroupType(), DcGroupType.MASTER)) {
                throw new BadRequestException(String.format("active dc %d of cluster %s must be DRMaster",
                        clusterTbl.getActivedcId(), clusterTbl.getClusterName()));
            }
        });
    }

    @Override
    public List<DcClusterTbl> findAllByClusterAndGroupType(long clusterId, long dcId, String groupType) {
        if (DcGroupType.isNullOrDrMaster(groupType)) {
            return findClusterRelated(clusterId)
                    .stream()
                    .filter(dcClusterTbl -> DcGroupType.isNullOrDrMaster(dcClusterTbl.getGroupType()))
                    .collect(Collectors.toList());
        }
        List<DcClusterTbl> result = new ArrayList<>();
        DcClusterTbl dcClusterTbl = find(dcId, clusterId);
        if (dcClusterTbl != null) {
            result.add(dcClusterTbl);
        }
        return result;
    }

    @Override
    public DcClusterTbl addDcCluster(String dcName, String clusterName, String redisConfigRule) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<DcClusterTbl> findByClusterIds(List<Long> clusterIds) {
        if (clusterIds == null || clusterIds.isEmpty()) {
            return Collections.emptyList();
        }
        return allDcClusters.getData().stream()
                .filter(dcCluster -> clusterIds.contains(dcCluster.getClusterId()))
                .collect(Collectors.toList());
    }

    @Override
    public List<DcClusterTbl> findAllByDcId(long dcId) {
        return allDcClusters.getData().stream()
                .filter(dcCluster -> dcCluster.getDcId() == dcId)
                .collect(Collectors.toList());
    }

    @Override
    public List<DcClusterTbl> findClusterRelated(long clusterId) {
        ClusterTbl clusterTbl = clusterService.find(clusterId);
        if (clusterTbl == null) {
            return Collections.emptyList();
        }
        return findClusterRelated(clusterTbl.getClusterName()).stream()
                .map(info -> toDcClusterTbl(info, clusterTbl))
                .collect(Collectors.toList());
    }

    @Override
    public List<DcClusterCreateInfo> findClusterRelated(String clusterName) {
        List<DcClusterCreateInfo> infos = consolePortalService.getDcClusterInfoOfCluster(clusterName);
        return infos == null ? Collections.emptyList() : infos;
    }

    @Override
    public DcClusterModel findDcClusterModelByClusterAndDc(String clusterName, String dcName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<DcClusterModel> findDcClusterModelsByCluster(String clusterName) {
        throw new UnsupportedOperationException();
    }
}
