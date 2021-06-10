package com.ctrip.xpipe.redis.console.service.migration.cmd.beacon;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.migration.manager.MigrationEventManager;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterMigrationNotSuccessException;

import java.util.concurrent.Executor;

/**
 * @author lishanglin
 * date 2021/4/17
 */
public class MigrationDoExecuteCmd extends AbstractMigrationCmd<Boolean> implements Observer {

    private MigrationEventManager migrationEventManager;

    private Executor migrationExecutor;

    public MigrationDoExecuteCmd(BeaconMigrationRequest migrationRequest, MigrationEventManager migrationEventManager, Executor migrationExecutor) {
        super(migrationRequest);
        this.migrationEventManager = migrationEventManager;
        this.migrationExecutor = migrationExecutor;
    }

    @Override
    protected void innerExecute() throws Throwable {
        BeaconMigrationRequest migrationRequest = getMigrationRequest();
        MigrationEvent event = migrationEventManager.getEvent(migrationRequest.getMigrationEventId());
        long clusterId = migrationRequest.getClusterTbl().getId();

        migrationExecutor.execute(() -> {
            try {
                event.getMigrationCluster(clusterId).addObserver(this);
                event.getMigrationCluster(clusterId).process();
            } catch (Throwable th) {
                future().setFailure(th);
            }
        });
    }

    @Override
    public void update(Object args, Observable observable) {
        MigrationCluster migrationCluster = (MigrationCluster) observable;
        MigrationStatus migrationStatus = migrationCluster.getStatus();

        if (migrationStatus.isTerminated() || migrationStatus.isPaused()) {
            if (MigrationStatus.Success.equals(migrationStatus)) {
                future().setSuccess(true);
            } else {
                future().setFailure(new ClusterMigrationNotSuccessException(migrationCluster.clusterName(), migrationStatus));
            }

            observable.removeObserver(this);
        }
    }

}
