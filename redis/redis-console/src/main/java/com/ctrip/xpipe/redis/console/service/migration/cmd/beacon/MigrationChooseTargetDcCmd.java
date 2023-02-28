package com.ctrip.xpipe.redis.console.service.migration.cmd.beacon;

import com.ctrip.xpipe.redis.checker.DcRelationsService;
import com.ctrip.xpipe.redis.console.cache.DcCache;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.migration.exception.*;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/4/17
 */
public class MigrationChooseTargetDcCmd extends AbstractMigrationCmd<DcTbl> {

    private DcCache dcCache;

    private DcClusterService dcClusterService;

    private DcRelationsService dcRelationsService;

    private static final Logger logger = LoggerFactory.getLogger(MigrationChooseTargetDcCmd.class);

    public MigrationChooseTargetDcCmd(BeaconMigrationRequest migrationRequest, DcCache dcCache, DcClusterService dcClusterService, DcRelationsService dcRelationsService) {
        super(migrationRequest);
        this.dcCache = dcCache;
        this.dcClusterService = dcClusterService;
        this.dcRelationsService = dcRelationsService;
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
        if (null != forcedDc && null == currentEventTargetDc && forcedDc.getZoneId() != sourcedDc.getZoneId()) {
            logger.info("[doExecute][{}] cross zone migration is not allow for {}", clusterName, migrationRequest.getTargetIDC());
            future().setFailure(new MigrationCrossZoneException(clusterName, sourcedDc.getDcName(), forcedDc.getDcName()));
            return;
        }

        if (null != forcedDc) {
            migrationRequest.setTargetDcTbl(forcedDc);
            future().setSuccess(forcedDc);
        } else if (null != currentEventTargetDc) {
            Set<String> availableDcs = migrationRequest.getAvailableDcs();
            if (!availableDcs.contains(currentEventTargetDc.getDcName())) {
                logger.info("[doExecute][{}] current dest dc {} is not available", clusterName, currentEventTargetDc.getDcName());
                future().setFailure(new MigrationConflictException(clusterName, null, currentEventTargetDc.getDcName()));
                return;
            }

            migrationRequest.setTargetDcTbl(currentEventTargetDc);
            future().setSuccess(currentEventTargetDc);
        } else {
            Set<String> availableDcs = migrationRequest.getAvailableDcs();
            if (availableDcs.isEmpty()) {
                future().setFailure(new NoAvailableDcException(clusterName));
                return;
            }

            List<String> targetDcs = dcRelationsService.getTargetDcsByPriority(clusterName, sourcedDc.getDcName(), Lists.newArrayList(availableDcs));
            int dcCount = targetDcs.size();
            long clusterId = cluster.getId();
            int index = (int) (clusterId % dcCount);
            String targetDcName = targetDcs.get(index);
            DcTbl targetDc = dcCache.find(targetDcName);
            if (null == targetDc) {
                future().setFailure(new UnknownTargetDcException(clusterName, targetDcName));
                return;
            }

            migrationRequest.setTargetDcTbl(targetDc);
            future().setSuccess(targetDc);
        }
    }

}
