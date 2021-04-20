package com.ctrip.xpipe.redis.console.service.migration.cmd.beacon;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.dao.MigrationEventDao;
import com.ctrip.xpipe.redis.console.migration.manager.MigrationEventManager;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.service.migration.impl.MigrationRequest;

/**
 * @author lishanglin
 * date 2021/4/18
 */
public class MigrationBuildEventCmd extends AbstractCommand<Long> {

    private BeaconMigrationRequest migrationRequest;

    private MigrationEventDao migrationEventDao;

    private MigrationEventManager migrationEventManager;

    private static final String MIGRATION_OPERATOR = "Beacon";

    public MigrationBuildEventCmd(BeaconMigrationRequest migrationRequest, MigrationEventDao migrationEventDao,
                                  MigrationEventManager migrationEventManager) {
        this.migrationRequest = migrationRequest;
        this.migrationEventDao = migrationEventDao;
        this.migrationEventManager = migrationEventManager;
    }

    @Override
    protected void doExecute() throws Throwable {
        MigrationClusterTbl currentMigrationTbl = migrationRequest.getCurrentMigrationCluster();
        if (null != currentMigrationTbl) {
            migrationRequest.setMigrationEventId(currentMigrationTbl.getMigrationEventId());
            future().setSuccess(currentMigrationTbl.getMigrationEventId());
            return;
        }

        MigrationEvent event = migrationEventDao.createMigrationEvent(buildXPipeMigrationRequest());
        migrationEventManager.addEvent(event);
        migrationRequest.setMigrationEventId(event.getMigrationEventId());
        future().setSuccess(event.getMigrationEventId());
    }

    private MigrationRequest buildXPipeMigrationRequest() {
        ClusterTbl cluster = migrationRequest.getClusterTbl();
        DcTbl sourceDc = migrationRequest.getSourceDcTbl();
        DcTbl targetDc = migrationRequest.getTargetDcTbl();

        MigrationRequest request = new MigrationRequest(MIGRATION_OPERATOR);
        request.setTag(MIGRATION_OPERATOR);
        MigrationRequest.ClusterInfo migrationCluster = new MigrationRequest.ClusterInfo(cluster.getId(), cluster.getClusterName(),
                sourceDc.getId(), sourceDc.getDcName(), targetDc.getId(), targetDc.getDcName());
        request.addClusterInfo(migrationCluster);

        return request;
    }

    @Override
    protected void doReset() {
        // do nothing
    }

    @Override
    public String getName() {
        if (null != migrationRequest) return "MigrationDoBuildCmd-" + migrationRequest.getClusterName();
        else return "MigrationDoBuildCmd-unknown";
    }
}
