package com.ctrip.xpipe.redis.console.migration.model;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public interface MigrationCluster extends Observer, Observable, MigrationClusterInfoHolder, MigrationClusterAction, MigrationClusterServiceHolder {

    Executor getMigrationExecutor();

    ScheduledExecutorService getScheduled();

    MigrationEvent getMigrationEvent();

    String fromDc();
    String destDc();

    long fromDcId();
    long destDcId();

    void updatePublishInfo(String desc);

    void updateActiveDcIdToDestDcId();

    ClusterStepResult stepStatus(ShardMigrationStep shardMigrationStep);

    void markCheckFail(String failMessage);

    OuterClientService getOuterClientService();

}
