package com.ctrip.xpipe.redis.console.migration.model.impl;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.observer.AbstractObservable;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.MigrationEventTbl;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterNotFoundException;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author shyin
 *         <p>
 *         Dec 8, 2016
 */
public class DefaultMigrationEvent extends AbstractObservable implements MigrationEvent, Observer {
    private MigrationEventTbl event;
    private Map<Long, MigrationCluster> migrationClusters = new HashMap<>();

    public DefaultMigrationEvent(MigrationEventTbl event) {
        this.event = event;
    }

    @Override
    public MigrationEventTbl getEvent() {
        return event;
    }

    @Override
    public void process() {
        List<MigrationCluster> localMigrationClusters = getMigrationClusters();
        if (localMigrationClusters.isEmpty()) {
            logger.info("[process][{}][no cluster]{}", event.getId(), localMigrationClusters);
            return;
        }

        MigrationCluster migrationCluster = localMigrationClusters.get(0);
        migrationCluster.process();
    }

    @Override
    public long getMigrationEventId() {
        return event.getId();
    }

    @Override
    public MigrationCluster getMigrationCluster(long clusterId) {
        return migrationClusters.get(clusterId);
    }

    @Override
    public MigrationCluster getMigrationCluster(String clusterName) {

        for(MigrationCluster migrationCluster : getMigrationClusters()){
            if(migrationCluster.clusterName().equals(clusterName)){
                return migrationCluster;
            }
        }
        return null;
    }

    @Override
    public MigrationCluster rollbackCluster(long clusterId) throws ClusterNotFoundException {
        MigrationCluster migrationCluster = getMigrationCluster(clusterId);
        if(migrationCluster == null){
            throw new ClusterNotFoundException(clusterId);
        }
        migrationCluster.rollback();

        return migrationCluster;
    }

        @Override
    public MigrationCluster rollbackCluster(String clusterName) throws ClusterNotFoundException {
        MigrationCluster cluster = getMigrationCluster(clusterName);
        if(cluster == null) {
            throw new ClusterNotFoundException(clusterName);
        }
        cluster.rollback();

        return cluster;
    }

    @Override
    public List<MigrationCluster> getMigrationClusters() {
        return Lists.newLinkedList(migrationClusters.values());
    }

    @Override
    public void addMigrationCluster(MigrationCluster migrationClsuter) {
        migrationClsuter.addObserver(this);
        migrationClusters.put(migrationClsuter.getMigrationCluster().getClusterId(), migrationClsuter);
    }

    @Override
    public void update(Object args, Observable observable) {
        if (args instanceof MigrationCluster) {
            MigrationStatus status = ((MigrationCluster) args).getStatus();
            if (status.isTerminated() && MigrationStatus.TYPE_SUCCESS.equals(status.getType())) {
                // Submit next task according to policy
                processNext();
            }
        }
        int finishedCnt = 0;
        for (MigrationCluster cluster : migrationClusters.values()) {
            if (cluster.getStatus().isTerminated()) {
                ++finishedCnt;
            }
        }
        if (finishedCnt == migrationClusters.size()) {
            notifyObservers(this);
        }
    }

    private void processNext() {

        for (MigrationCluster migrationCluster : migrationClusters.values()) {
            if (!migrationCluster.getStatus().isTerminated()) {
                migrationCluster.process();
            }
        }
    }

    @Override
    public boolean isDone() {

        int successCnt = 0;
        List<MigrationCluster> migrationClusters = getMigrationClusters();
        for (MigrationCluster cluster : migrationClusters) {
            if (cluster.getStatus().isTerminated()) {
                ++successCnt;
            }
        }

        if (successCnt == migrationClusters.size()) {
            logger.debug("[isDone][true]{}, success:{}", getMigrationEventId(), successCnt);
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format("[eventId:%d]", getMigrationEventId());
    }
}
