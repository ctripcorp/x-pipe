package com.ctrip.xpipe.redis.console.service.migration.cmd.beacon;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.console.cache.DcCache;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.migration.MigrationSystemAvailableChecker;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import com.ctrip.xpipe.redis.console.service.migration.exception.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lishanglin
 * date 2021/4/17
 */
public class MigrationPreCheckCmd extends AbstractCommand<Boolean> {

    private MigrationSystemAvailableChecker checker;

    private ConfigService configService;

    private ClusterService clusterService;

    private DcCache dcCache;

    private BeaconMetaService beaconMetaService;

    private BeaconMigrationRequest migrationRequest;

    private static final Logger logger = LoggerFactory.getLogger(MigrationPreCheckCmd.class);

    public MigrationPreCheckCmd(BeaconMigrationRequest migrationRequest, MigrationSystemAvailableChecker checker, ConfigService configService,
                                ClusterService clusterService, DcCache dcCache, BeaconMetaService beaconMetaService) {
        this.migrationRequest = migrationRequest;
        this.checker = checker;
        this.configService = configService;
        this.clusterService = clusterService;
        this.dcCache = dcCache;
        this.beaconMetaService = beaconMetaService;
    }

    @Override
    protected void doExecute() throws Throwable {
        if(!checker.getResult().isAvaiable() && !configService.ignoreMigrationSystemAvailability()) {
            future().setFailure(new MigrationSystemNotHealthyException(checker.getResult().getMessage()));
            return;
        }

        String clusterName = migrationRequest.getClusterName();
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        if (null == clusterTbl) {
            future().setFailure(new ClusterNotFoundException(clusterName));
            return;
        }
        if (!ClusterType.lookup(clusterTbl.getClusterType()).supportMigration()) {
            future().setFailure(new MigrationNotSupportException(clusterName));
            return;
        }

        boolean forcedMigration = migrationRequest.getIsForced();
        DcTbl activeDc = dcCache.find(clusterTbl.getActivedcId());
        if (forcedMigration) {
            logger.info("[checkMetaAndAvailableDcs][{}] skip for forced migration", clusterName);
        } else if (!beaconMetaService.compareMetaWithXPipe(clusterName, migrationRequest.getGroups())) {
            future().setFailure(new WrongClusterMetaException(clusterName));
            return;
        } else if (!migrationRequest.getFailDcs().contains(activeDc.getDcName())) {
            future().setFailure(new MigrationNoNeedException(clusterName));
            return;
        }

        migrationRequest.setClusterTbl(clusterTbl);
        migrationRequest.setSourceDcTbl(activeDc);
        future().setSuccess(true);
    }

    @Override
    protected void doReset() {
        // do nothing
    }

    @Override
    public String getName() {
        if (null != migrationRequest) return "MigrationPreCheckCmd-" + migrationRequest.getClusterName();
        else return "MigrationPreCheckCmd-unknown";
    }
}
