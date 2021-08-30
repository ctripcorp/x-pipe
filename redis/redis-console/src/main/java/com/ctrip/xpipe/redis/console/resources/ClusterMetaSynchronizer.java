package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.model.ClusterModel;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

public class ClusterMetaSynchronizer {
    private static Logger logger = LoggerFactory.getLogger(ClusterMetaSynchronizer.class);
    private Set<ClusterMeta> added;
    private Set<ClusterMeta> removed;
    private Set<MetaComparator> modified;
    private ClusterService clusterService;
    private ShardService shardService;
    private RedisService redisService;
    private DcService dcService;
    private OrganizationService organizationService;

    public ClusterMetaSynchronizer(Set<ClusterMeta> added, Set<ClusterMeta> removed, Set<MetaComparator> modified, DcService dcService, ClusterService clusterService, ShardService shardService, RedisService redisService) {
        this.added = added;
        this.removed = removed;
        this.modified = modified;
        this.dcService = dcService;
        this.clusterService = clusterService;
        this.shardService = shardService;
        this.redisService = redisService;
    }

    public void sync() {
        remove();
        add();
        update();
    }

    void remove() {
        try {
            removed.forEach(clusterMeta -> {
                try {
                    clusterService.deleteCluster(clusterMeta.getId());
                } catch (Exception e) {
                    logger.error("ClusterMetaSynchronizer.remove:{}", clusterMeta.toString(), e);
                }
            });
        } catch (Exception e) {
            logger.error("ClusterMetaSynchronizer.remove", e);
        }
    }

    void add() {
        try {
            long currentDcId = dcService.find(DcMetaSynchronizer.currentDcId).getId();
            if (!added.isEmpty()) {
                added.forEach(clusterMeta -> {
                    try {
                        ClusterTbl clusterTbl = new ClusterTbl().setClusterName(clusterMeta.getId()).setClusterType(clusterMeta.getType()).setClusterAdminEmails(clusterMeta.getAdminEmails())
                                .setClusterOrgId(clusterMeta.getOrgId()).setClusterDescription(clusterMeta.getId())
                                .setClusterOrgName(organizationService.getOrganization(clusterMeta.getOrgId()).getOrgName());
                        if (ClusterType.lookup(clusterMeta.getType()).supportSingleActiveDC()) {
                            clusterTbl.setActivedcId(currentDcId);
                        }
                        ClusterModel clusterModel = new ClusterModel().setClusterTbl(clusterTbl);
                        if (ClusterType.lookup(clusterMeta.getType()).supportMultiActiveDC()) {
                            clusterModel.setDcs(Lists.newArrayList(new DcTbl().setId(currentDcId).setDcName(DcMetaSynchronizer.currentDcId)));
                        }
                        clusterService.createCluster(clusterModel);
                        Map<String, ShardMeta> shardMetaMap = clusterMeta.getShards();
                        new ShardMetaSynchronizer(Sets.newHashSet(shardMetaMap.values()), null, null, redisService, shardService).sync();
                    } catch (Exception e) {
                        logger.error("ClusterMetaSynchronizer.add:{}", clusterMeta.toString(), e);
                    }
                });
            }
        } catch (Exception e) {
            logger.error("ClusterMetaSynchronizer.add", e);
        }
    }

    void update() {
        try {
            long currentDcId = dcService.find(DcMetaSynchronizer.currentDcId).getId();
            modified.forEach(metaComparator -> {
                try {
                    ClusterMetaComparator clusterMetaComparator = (ClusterMetaComparator) metaComparator;
                    ClusterMeta future = clusterMetaComparator.getFuture();
                    ClusterTbl current = clusterService.find(future.getId());
                    if (needUpdate(future, current, currentDcId)) {
                        if (current.getClusterOrgId() != future.getOrgId()) {
                            current.setClusterOrgId(future.getOrgId()).setClusterOrgName(organizationService.getOrganization(future.getOrgId()).getOrgName());
                        }
                        current.setClusterType(future.getType()).setClusterAdminEmails(future.getAdminEmails());
                        if (ClusterType.lookup(future.getType()).supportSingleActiveDC()) {
                            current.setActivedcId(currentDcId);
                        }
                        clusterService.update(current);
                    }
                    new ShardMetaSynchronizer(clusterMetaComparator.getAdded(), clusterMetaComparator.getRemoved(), clusterMetaComparator.getMofified(), redisService, shardService).sync();
                } catch (Exception e) {
                    logger.error("ClusterMetaSynchronizer.update:{}->{}", ((ClusterMetaComparator) metaComparator).getCurrent(), ((ClusterMetaComparator) metaComparator).getFuture(), e);
                }
            });
        } catch (Exception e) {
            logger.error("ClusterMetaSynchronizer.update", e);
        }
    }

    boolean needUpdate(ClusterMeta future, ClusterTbl current, long currentDcId) {
        return !(current.getClusterName().equals(future.getId()) &&
                current.getClusterOrgId() == future.getOrgId() &&
                current.getClusterAdminEmails().equals(future.getAdminEmails()) &&
                current.getClusterType().equals(future.getType()) &&
                current.getActivedcId() == currentDcId);
    }

}
