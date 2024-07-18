package com.ctrip.xpipe.redis.console.service.migration.cmd.beacon;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.cache.DcCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
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
public class MigrationPreCheckCmd extends AbstractMigrationCmd<Boolean> {

    private MigrationSystemAvailableChecker checker;

    private ConfigService configService;

    private ClusterService clusterService;

    private DcCache dcCache;

    private BeaconMetaService beaconMetaService;

    private ConsoleConfig config;

    private static final Logger logger = LoggerFactory.getLogger(MigrationPreCheckCmd.class);

    public MigrationPreCheckCmd(BeaconMigrationRequest migrationRequest, MigrationSystemAvailableChecker checker, ConfigService configService,
                                ClusterService clusterService, DcCache dcCache, BeaconMetaService beaconMetaService, ConsoleConfig config) {
        super(migrationRequest);
        this.checker = checker;
        this.configService = configService;
        this.clusterService = clusterService;
        this.dcCache = dcCache;
        this.beaconMetaService = beaconMetaService;
        this.config = config;
    }

    @Override
    protected void innerExecute() throws Throwable {
        if (!configService.allowAutoMigration()) {
            future().setFailure(new AutoMigrationNotAllowException());
            return;
        }
        if(!checker.getResult().isAvaiable() && !configService.ignoreMigrationSystemAvailability()) {
            future().setFailure(new MigrationSystemNotHealthyException(checker.getResult().getMessage()));
            return;
        }

        BeaconMigrationRequest migrationRequest = getMigrationRequest();
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

}
