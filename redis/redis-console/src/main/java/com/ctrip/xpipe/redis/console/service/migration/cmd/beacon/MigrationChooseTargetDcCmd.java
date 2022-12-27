package com.ctrip.xpipe.redis.console.service.migration.cmd.beacon;

import com.ctrip.xpipe.redis.console.cache.DcCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.DcRelation;
import com.ctrip.xpipe.redis.console.config.DcsRelationsInfo;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.migration.exception.*;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author lishanglin
 * date 2021/4/17
 */
public class MigrationChooseTargetDcCmd extends AbstractMigrationCmd<DcTbl> {

    private DcCache dcCache;

    private DcClusterService dcClusterService;

    private ConsoleConfig config;

    private static final Logger logger = LoggerFactory.getLogger(MigrationChooseTargetDcCmd.class);

    public MigrationChooseTargetDcCmd(BeaconMigrationRequest migrationRequest, DcCache dcCache, DcClusterService dcClusterService, ConsoleConfig config) {
        super(migrationRequest);
        this.dcCache = dcCache;
        this.dcClusterService = dcClusterService;
        this.config = config;
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

            String targetDcName = getTargetDcName(sourcedDc.getDcName(), availableDcs);
            DcTbl targetDc;
            if (targetDcName == null || (targetDc = dcCache.find(targetDcName)) == null) {
                future().setFailure(new UnknownTargetDcException(clusterName, targetDcName));
                return;
            }

            migrationRequest.setTargetDcTbl(targetDc);
            future().setSuccess(targetDc);
        }
    }

    @VisibleForTesting
    protected String getTargetDcName(String srcDcName, Set<String> availableDcs) {
        DcsRelationsInfo dcsRelationsInfo = config.getDcsRelationsInfo();
        if (dcsRelationsInfo == null || dcsRelationsInfo.getRelations() == null) {
            return availableDcs.iterator().next();
        }

        Set<String> result = new HashSet<>();
        int currentPriority = Integer.MAX_VALUE;
        for (DcRelation dcRelation : dcsRelationsInfo.getRelations()) {
            String[] dcs = dcRelation.getDcs().split("\\s*,\\s*");
            boolean first = dcs[0].equalsIgnoreCase(srcDcName) && availableDcs.contains(dcs[1]);
            boolean second = dcs[1].equalsIgnoreCase(srcDcName) && availableDcs.contains(dcs[0]);
            if ((first || second) && dcRelation.getDistance() <= currentPriority && dcRelation.getDistance() > 0) {
                if (dcRelation.getDistance() < currentPriority) {
                    result.clear();
                    currentPriority = dcRelation.getDistance();
                }
                result.add(first ? dcs[1] : dcs[0]);
            }
        }

        return result.isEmpty() ? null : new ArrayList<String>(result).get(new Random().nextInt(result.size()));
    }
}