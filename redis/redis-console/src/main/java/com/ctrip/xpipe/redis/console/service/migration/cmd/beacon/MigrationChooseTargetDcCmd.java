package com.ctrip.xpipe.redis.console.service.migration.cmd.beacon;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.DcRelationsService;
import com.ctrip.xpipe.redis.console.cache.DcCache;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.migration.exception.*;
import com.ctrip.xpipe.redis.console.service.migration.support.HeteroMigrationSupport;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * @author lishanglin
 * date 2021/4/17
 */
public class MigrationChooseTargetDcCmd extends AbstractMigrationCmd<DcTbl> {

    private DcCache dcCache;

    private DcClusterService dcClusterService;

    private DcRelationsService dcRelationsService;

    private HeteroMigrationSupport heteroMigrationSupport;

    private static final Logger logger = LoggerFactory.getLogger(MigrationChooseTargetDcCmd.class);

    public MigrationChooseTargetDcCmd(BeaconMigrationRequest migrationRequest, DcCache dcCache,
                                      DcClusterService dcClusterService, DcRelationsService dcRelationsService,
                                      HeteroMigrationSupport heteroMigrationSupport) {
        super(migrationRequest);
        this.dcCache = dcCache;
        this.dcClusterService = dcClusterService;
        this.dcRelationsService = dcRelationsService;
        this.heteroMigrationSupport = heteroMigrationSupport;
    }

    @Override
    protected void innerExecute() throws Throwable {
        BeaconMigrationRequest migrationRequest = getMigrationRequest();
        ClusterTbl cluster = getMigrationRequest().getClusterTbl();
        String clusterName = cluster.getClusterName();
        DcTbl sourcedDc = getMigrationRequest().getSourceDcTbl();
        DcTbl forcedDc = getMigrationRequest().getIsForced() ? dcCache.find(migrationRequest.getTargetIDC()) : null;
        DcTbl currentEventTargetDc = null != migrationRequest.getCurrentMigrationCluster() ?
                dcCache.find(migrationRequest.getCurrentMigrationCluster().getDestinationDcId()) : null;
        boolean heteroCluster = heteroMigrationSupport.isHeteroCluster(cluster);
        AzGroupClusterEntity azGroupCluster = migrationRequest.getAzGroupCluster();

        if (migrationRequest.getIsForced() && null == forcedDc) {
            logger.info("[doExecute][{}] unknown forced target dc {}", clusterName, migrationRequest.getTargetIDC());
            future().setFailure(new UnknownTargetDcException(clusterName, migrationRequest.getTargetIDC()));
            return;
        }

        if (null != forcedDc && null != currentEventTargetDc && forcedDc.getId() != currentEventTargetDc.getId()) {
            logger.warn("[doExecute][{}] target dc conflict, forced {}, current {}", clusterName, forcedDc.getDcName(), currentEventTargetDc.getDcName());
            future().setFailure(new MigrationConflictException(clusterName, forcedDc.getDcName(), currentEventTargetDc.getDcName()));
            return;
        }
        if (null != forcedDc && null == currentEventTargetDc && forcedDc.getId() == sourcedDc.getId()) {
            logger.info("[doExecute][{}] forced dc {} is current active dc", clusterName, migrationRequest.getTargetIDC());
            future().setFailure(new MigrationNoNeedException(clusterName));
            return;
        }
        if (null != forcedDc && null == currentEventTargetDc && null == dcClusterService.find(forcedDc.getId(), cluster.getId())) {
            logger.info("[doExecute][{}] cluster no dc {}", clusterName, migrationRequest.getTargetIDC());
            future().setFailure(new UnknownTargetDcException(clusterName, forcedDc.getDcName()));
            return;
        }
        if (null != forcedDc && null == currentEventTargetDc && !isTargetDcAllowed(cluster, sourcedDc, forcedDc, heteroCluster)) {
            logger.info("[doExecute][{}] cross scope migration is not allow for {}", clusterName, migrationRequest.getTargetIDC());
            future().setFailure(new MigrationCrossZoneException(clusterName, sourcedDc.getDcName(), forcedDc.getDcName()));
            return;
        }

        if (null != forcedDc) {
            migrationRequest.setTargetDcTbl(forcedDc);
            future().setSuccess(forcedDc);
        } else if (null != currentEventTargetDc) {
            Set<String> availableDcs = filterAvailableDcs(migrationRequest.getAvailableDcs(), azGroupCluster);
            if (!availableDcs.contains(currentEventTargetDc.getDcName())) {
                logger.info("[doExecute][{}] current dest dc {} is not available", clusterName, currentEventTargetDc.getDcName());
                future().setFailure(new MigrationConflictException(clusterName, null, currentEventTargetDc.getDcName()));
                return;
            }

            migrationRequest.setTargetDcTbl(currentEventTargetDc);
            future().setSuccess(currentEventTargetDc);
        } else {
            Set<String> availableDcs = filterAvailableDcs(migrationRequest.getAvailableDcs(), azGroupCluster);
            if (availableDcs.isEmpty()) {
                future().setFailure(new NoAvailableDcException(clusterName));
                return;
            }

            String targetDcName = dcRelationsService.getClusterTargetDcByPriority(cluster.getId(), clusterName, sourcedDc.getDcName(), Lists.newArrayList(availableDcs));
            if (targetDcName == null) {
                logger.info("[doExecute][{}] refused to migrate from {} to {}", clusterName, sourcedDc.getDcName(), availableDcs);
                future().setFailure(new NoAvailableDcException(clusterName));
                return;
            }

            DcTbl targetDc = dcCache.find(targetDcName);
            if (null == targetDc) {
                future().setFailure(new UnknownTargetDcException(clusterName, targetDcName));
                return;
            }

            migrationRequest.setTargetDcTbl(targetDc);
            future().setSuccess(targetDc);
        }
    }

    private Set<String> filterAvailableDcs(Set<String> availableDcs, AzGroupClusterEntity azGroupCluster) {
        if (azGroupCluster == null) {
            return availableDcs;
        }
        return heteroMigrationSupport.filterDcsInSameAzGroup(azGroupCluster, availableDcs);
    }

    private boolean isTargetDcAllowed(ClusterTbl cluster, DcTbl sourceDc, DcTbl targetDc, boolean heteroCluster) {
        if (heteroCluster) {
            return heteroMigrationSupport.isSameAzGroup(cluster.getId(), sourceDc.getDcName(), targetDc.getDcName());
        }
        return targetDc.getZoneId() == sourceDc.getZoneId();
    }

}
