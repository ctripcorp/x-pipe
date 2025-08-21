package com.ctrip.xpipe.redis.console.service.migration.cmd.beacon;

import com.ctrip.xpipe.redis.console.cache.DcCache;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.dao.MigrationClusterDao;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.migration.exception.MigrationJustFinishException;
import com.ctrip.xpipe.redis.console.service.migration.exception.UnexpectMigrationDataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lishanglin
 * date 2021/4/17
 */
public class MigrationFetchProcessingEventCmd extends AbstractMigrationCmd<MigrationClusterTbl> {

    private MigrationClusterDao migrationClusterDao;

    private ClusterService clusterService;

    private DcCache dcCache;

    private static final Logger logger = LoggerFactory.getLogger(MigrationFetchProcessingEventCmd.class);

    public MigrationFetchProcessingEventCmd(BeaconMigrationRequest migrationRequest, ClusterService clusterService,
                                            MigrationClusterDao migrationClusterDao, DcCache dcCache) {
        super(migrationRequest);
        this.clusterService = clusterService;
        this.migrationClusterDao = migrationClusterDao;
        this.dcCache = dcCache;
    }

    @Override
    protected void innerExecute() throws Throwable {
        BeaconMigrationRequest migrationRequest = getMigrationRequest();
        ClusterTbl clusterTbl = migrationRequest.getClusterTbl();
        if (ClusterStatus.isSameClusterStatus(clusterTbl.getStatus(), ClusterStatus.Normal)) {
            logger.info("[doExecute][{}] no processing migration, skip", clusterTbl.getClusterName());
            future().setSuccess(null);
            return;
        }

        MigrationClusterTbl currentMigration = migrationClusterDao.findByEventIdAndClusterId(clusterTbl.getMigrationEventId(), clusterTbl.getId());
        if (null == currentMigration) {
            logger.warn("[doExecute][{}] status {} but no migration {}", clusterTbl.getClusterName(), clusterTbl.getStatus(), clusterTbl.getMigrationEventId());
            future().setFailure(new UnexpectMigrationDataException(clusterTbl, "on migration but no event"));
            return;
        }

        MigrationStatus status = MigrationStatus.valueOf(currentMigration.getStatus());
        if (status.isTerminated()) {
            ClusterTbl currentClusterTbl = clusterService.find(migrationRequest.getClusterName());
            boolean clusterStillOnMigration = !ClusterStatus.isSameClusterStatus(currentClusterTbl.getStatus(), ClusterStatus.Normal);

            if (currentClusterTbl.getMigrationEventId() == clusterTbl.getMigrationEventId() && clusterStillOnMigration) {
                logger.warn("[doExecute][{}] status {} but event {} has finished", clusterTbl.getClusterName(), clusterTbl.getStatus(), clusterTbl.getMigrationEventId());
                future().setFailure(new UnexpectMigrationDataException(clusterTbl, "on migration but event finish"));
                return;
            } else {
                logger.warn("[doExecute][{}] just finish migration {}", clusterTbl.getClusterName(), clusterTbl.getMigrationEventId());
                future().setFailure(new MigrationJustFinishException(migrationRequest.getClusterName(), dcCache.find(currentClusterTbl.getActivedcId()).getDcName()));
            }
        }

        // migration is on processing
        migrationRequest.setCurrentMigrationCluster(currentMigration);
        future().setSuccess(currentMigration);
    }

}
