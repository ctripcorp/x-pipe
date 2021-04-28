package com.ctrip.xpipe.redis.console.service.migration.cmd.beacon;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.migration.manager.MigrationEventManager;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterMigrationNotSuccessException;

/**
 * @author lishanglin
 * date 2021/4/17
 */
public class MigrationDoExecuteCmd extends AbstractCommand<Boolean> implements Observer {

    private BeaconMigrationRequest migrationRequest;

    private MigrationEventManager migrationEventManager;

    public MigrationDoExecuteCmd(BeaconMigrationRequest migrationRequest, MigrationEventManager migrationEventManager) {
        this.migrationRequest = migrationRequest;
        this.migrationEventManager = migrationEventManager;
    }

    @Override
    protected void doExecute() throws Throwable {
        MigrationEvent event = migrationEventManager.getEvent(migrationRequest.getMigrationEventId());
        long clusterId = migrationRequest.getClusterTbl().getId();

        event.getMigrationCluster(clusterId).addObserver(this);
        event.processCluster(clusterId);
    }

    @Override
    public void update(Object args, Observable observable) {
        MigrationCluster migrationCluster = (MigrationCluster) observable;
        MigrationStatus migrationStatus = migrationCluster.getStatus();

        if (!migrationCluster.isStarted() || migrationStatus.isTerminated()) {
            if (MigrationStatus.Success.equals(migrationStatus)) {
                future().setSuccess(true);
            } else {
                future().setFailure(new ClusterMigrationNotSuccessException(migrationCluster.clusterName(), migrationStatus));
            }

            observable.removeObserver(this);
        }
    }

    @Override
    protected void doReset() {
        // do nothing
    }

    @Override
    public String getName() {
        if (null != migrationRequest) return "MigrationDoExecuteCmd-" + migrationRequest.getClusterName();
        else return "MigrationDoExecuteCmd-unknown";
    }
}
