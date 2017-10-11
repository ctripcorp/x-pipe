package com.ctrip.xpipe.redis.console.migration.model;

import java.util.List;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.redis.console.model.MigrationEventTbl;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterNotFoundException;

/**
 * @author shyin
 *         <p>
 *         Dec 8, 2016
 */
public interface MigrationEvent extends Observable {

    MigrationEventTbl getEvent();

    void process();

    long getMigrationEventId();

    MigrationCluster getMigrationCluster(long clusterId);

    MigrationCluster getMigrationCluster(String clusterName);

    MigrationCluster rollbackCluster(long clusterId) throws ClusterNotFoundException;

    MigrationCluster rollbackCluster(String clusterName) throws ClusterNotFoundException;

    List<MigrationCluster> getMigrationClusters();

    void addMigrationCluster(MigrationCluster migrationCluster);

    boolean isDone();

}
