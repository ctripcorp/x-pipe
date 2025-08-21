package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.dto.ClusterCreateDTO;
import com.ctrip.xpipe.redis.console.dto.SingleGroupClusterCreateDTO;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.notifier.cluster.ClusterEvent;
import com.ctrip.xpipe.redis.console.notifier.cluster.ClusterTypeUpdateEventFactory;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterSyncMetaComparator;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.ctrip.xpipe.redis.core.meta.MetaSynchronizer.META_SYNC;

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
    private SentinelBalanceService sentinelBalanceService;
    private ConsoleConfig consoleConfig;
    private ClusterTypeUpdateEventFactory clusterTypeUpdateEventFactory;
    private String dcId;

    public ClusterMetaSynchronizer(Set<ClusterMeta> added, Set<ClusterMeta> removed, Set<MetaComparator> modified,
                                   DcService dcService, ClusterService clusterService, ShardService shardService,
                                   RedisService redisService, OrganizationService organizationService,
                                   SentinelBalanceService sentinelBalanceService, ConsoleConfig consoleConfig,
                                   ClusterTypeUpdateEventFactory clusterTypeUpdateEventFactory,
                                   String dcId
    ) {
        this.added = added;
        this.removed = removed;
        this.modified = modified;
        this.dcService = dcService;
        this.clusterService = clusterService;
        this.shardService = shardService;
        this.redisService = redisService;
        this.organizationService = organizationService;
        this.sentinelBalanceService = sentinelBalanceService;
        this.consoleConfig = consoleConfig;
        this.clusterTypeUpdateEventFactory = clusterTypeUpdateEventFactory;
        this.dcId = dcId;
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
                    List<DcTbl> relatedDcs = clusterService.getClusterRelatedDcs(clusterMeta.getId());
                    if (relatedDcs.size() == 0 || relatedDcs.size() == 1 && relatedDcs.get(0).getDcName().equalsIgnoreCase(dcId)) {
                        logger.info("[ClusterMetaSynchronizer][remove]{}, {}", clusterMeta, dcId);
                        clusterService.deleteCluster(clusterMeta.getId());
                        CatEventMonitor.DEFAULT.logEvent(META_SYNC, String.format("[delCluster]%s-%s", dcId, clusterMeta.getId()));
                    } else {
                        logger.info("[ClusterMetaSynchronizer][unbindDc]{}, {}", clusterMeta, dcId);
                        clusterService.unbindDc(clusterMeta.getId(), dcId);
                        CatEventMonitor.DEFAULT.logEvent(META_SYNC, String.format("[unbindDc]%s-%s", dcId, clusterMeta.getId()));
                    }
                } catch (Exception e) {
                    logger.error("[ClusterMetaSynchronizer][unbindDc]{}, {}", clusterMeta, dcId, e);
                }
            });
        } catch (Exception e) {
            logger.error("[ClusterMetaSynchronizer][remove]", e);
        }
    }

    void add() {
        try {

            if (!added.isEmpty()) {
                added.forEach(toAdd -> {
                    try {
                        ClusterTbl exist = clusterService.find(toAdd.getId());

                        if (exist == null) {
                            createCluster(toAdd);
                        } else if (shouldAddDc(toAdd, exist)) {
                            bindDc(toAdd);
                        } else {
                            logger.warn("[ClusterMetaSynchronizer][notBind]toAdd:{}, type:{}, exist:{}, type:{}", toAdd.getId(), toAdd.getType(), exist.getClusterName(), exist.getClusterType());
                            return;
                        }

                        new ShardMetaSynchronizer(Sets.newHashSet(toAdd.getShards().values()), null, null, redisService, shardService, sentinelBalanceService, consoleConfig, dcId).sync();
                    } catch (Exception e) {
                        logger.error("[ClusterMetaSynchronizer][add]{}", toAdd, e);
                    }
                });
            }
        } catch (Exception e) {
            logger.error("[ClusterMetaSynchronizer][add]", e);
        }
    }

    void bindDc(ClusterMeta toAdd){
        logger.info("[ClusterMetaSynchronizer][bindDc]{}, {}", toAdd, dcId);
        clusterService.bindDc(new DcClusterTbl().setClusterName(toAdd.getId()).setDcName(dcId));
        CatEventMonitor.DEFAULT.logEvent(META_SYNC, String.format("[bindDc]%s-%s", dcId, toAdd.getId()));
    }

    void createCluster(ClusterMeta toAdd){
        String orgName = null;
        if (toAdd.getOrgId() != null) {
            try {
                OrganizationTbl orgTbl = organizationService.getOrganization(toAdd.getOrgId());
                if (orgTbl != null) {
                    orgName = orgTbl.getOrgName();
                }
            } catch (Exception e) {
                logger.warn("[ClusterMetaSynchronizer][bindDc]orgId not found:{},{}", toAdd.getId(), toAdd.getOrgId());
            }
        }

        ClusterCreateDTO clusterCreateDTO = ClusterCreateDTO.builder()
            .clusterName(toAdd.getId())
            .clusterType(toAdd.getType())
            .activeAz(dcId)
            .description(toAdd.getId())
            .orgName(orgName)
            .adminEmails(toAdd.getAdminEmails())
            .build();
        SingleGroupClusterCreateDTO createDTO = new SingleGroupClusterCreateDTO(clusterCreateDTO,
            Collections.singletonList(dcId));

        logger.info("[ClusterMetaSynchronizer][createCluster]{}", toAdd);
        clusterService.createSingleGroupCluster(createDTO);

        CatEventMonitor.DEFAULT.logEvent(META_SYNC, String.format("[createCluster]%s-%s", dcId, toAdd.getId()));
    }

    boolean shouldAddDc(ClusterMeta toAdd, ClusterTbl exist) {
        return !existDiffTypeCluster(toAdd, exist);
    }

    boolean existDiffTypeCluster(ClusterMeta toAdd, ClusterTbl exist) {
        return exist != null && !toAdd.getType().equalsIgnoreCase(exist.getClusterType());
    }

    void update() {
        try {
            long currentDcId = dcService.find(dcId).getId();
            modified.forEach(metaComparator -> {
                try {
                    ClusterSyncMetaComparator clusterMetaComparator = (ClusterSyncMetaComparator) metaComparator;
                    ClusterMeta future = clusterMetaComparator.getFuture();
                    ClusterTbl currentClusterTbl = clusterService.find(future.getId());
                    String currentClusterTye = currentClusterTbl.getClusterType();
                    if (needUpdate(future, currentClusterTbl, currentDcId)) {
                        if (future.getOrgId() != null && currentClusterTbl.getClusterOrgId() != future.getOrgId()) {
                            currentClusterTbl.setClusterOrgId(future.getOrgId());
                            OrganizationTbl existedOrgTbl = organizationService.getOrganization(future.getOrgId());
                            if (existedOrgTbl != null)
                                currentClusterTbl.setClusterOrgName(existedOrgTbl.getOrgName());
                        }
                        currentClusterTbl.setClusterType(future.getType()).setClusterAdminEmails(future.getAdminEmails());
                        if (ClusterType.lookup(future.getType()).supportSingleActiveDC()) {
                            currentClusterTbl.setActivedcId(currentDcId);
                        } else {
                            currentClusterTbl.setActivedcId(0);
                        }
                        logger.info("[ClusterMetaSynchronizer][update]{} -> {}, toUpdateTbl: {}", clusterMetaComparator.getCurrent(), future, currentClusterTbl);
                        clusterService.update(currentClusterTbl);
                        CatEventMonitor.DEFAULT.logEvent(META_SYNC, String.format("[updateCluster]%s-%s", dcId, future.getId()));
                        notifyIfClusterTypeUpdated(currentClusterTye, currentClusterTbl);
                    }
                    new ShardMetaSynchronizer(clusterMetaComparator.getAdded(), clusterMetaComparator.getRemoved(), clusterMetaComparator.getMofified(),
                            redisService, shardService, sentinelBalanceService, consoleConfig, dcId).sync();
                } catch (Exception e) {
                    logger.error("[ClusterMetaSynchronizer][update]{} -> {}", ((ClusterMetaComparator) metaComparator).getCurrent(), ((ClusterMetaComparator) metaComparator).getFuture(), e);
                }
            });
        } catch (Exception e) {
            logger.error("[ClusterMetaSynchronizer][update]", e);
        }
    }

    void notifyIfClusterTypeUpdated(String originType, ClusterTbl currentClusterTbl) {
        try {
            if (!originType.equalsIgnoreCase(currentClusterTbl.getClusterType())) {
                ClusterEvent clusterEvent = clusterTypeUpdateEventFactory.createClusterEvent(currentClusterTbl.getClusterName(), currentClusterTbl);
                if (null != clusterEvent) clusterEvent.onEvent();
            }
        } catch (Throwable e) {
            logger.error("[ClusterMetaSynchronizer][notifyIfClusterTypeUpdated]{} -> {}", originType, currentClusterTbl.getClusterType(), e);
        }
    }

    boolean needUpdate(ClusterMeta future, ClusterTbl current, long currentDcId) {
        return !(Objects.equals(current.getClusterName(), future.getId()) &&
                Objects.equals(current.getClusterOrgId(), Long.valueOf(future.getOrgId())) &&
                Objects.equals(current.getClusterAdminEmails(), future.getAdminEmails()) &&
                Objects.equals(current.getClusterType(), future.getType()) &&
                Objects.equals(current.getActivedcId(), currentDcId));
    }

}
