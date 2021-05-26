package com.ctrip.xpipe.redis.console.migration.model;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.redis.console.model.MigrationEventTbl;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterNotFoundException;

import java.util.List;

/**
 * @author shyin
 *         <p>
 *         Dec 8, 2016
 */
public interface MigrationEvent extends Observable {

    MigrationEventTbl getEvent();

    void process() throws Exception;

    long getMigrationEventId();

    MigrationCluster getMigrationCluster(long clusterId);

    MigrationCluster getMigrationCluster(String clusterName);

    void processCluster(long clusterId) throws Exception;

    void cancelCluster(long clusterId) throws ClusterNotFoundException;

    void forceClusterProcess(long clusterId) throws ClusterNotFoundException;

    void forceClusterEnd(long clusterId) throws ClusterNotFoundException;

    MigrationCluster rollbackCluster(long clusterId) throws ClusterNotFoundException;

    MigrationCluster rollbackCluster(String clusterName) throws ClusterNotFoundException;

    List<MigrationCluster> getMigrationClusters();

    void addMigrationCluster(MigrationCluster migrationCluster);

    boolean isDone();

    boolean isRunning();

}
