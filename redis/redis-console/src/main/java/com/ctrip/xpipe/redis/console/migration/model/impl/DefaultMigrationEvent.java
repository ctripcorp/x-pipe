package com.ctrip.xpipe.redis.console.migration.model.impl;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.observer.AbstractObservable;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.model.MigrationLock;
import com.ctrip.xpipe.redis.console.model.MigrationEventTbl;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterNotFoundException;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author shyin
 *         <p>
 *         Dec 8, 2016
 */
public class DefaultMigrationEvent extends AbstractObservable implements MigrationEvent, Observer {
    private MigrationEventTbl event;
    private MigrationLock migrationLock;
    private Map<Long, MigrationCluster> migrationClusters = new HashMap<>();
    private AtomicBoolean running = new AtomicBoolean(false);

    public DefaultMigrationEvent(MigrationEventTbl event, MigrationLock migrationLock) {
        this.event = event;
        this.migrationLock = migrationLock;
    }

    @Override
    public MigrationEventTbl getEvent() {
        return event;
    }

    @Override
    public void process() {
        List<MigrationCluster> migrationClusters = getMigrationClusters();
        if (migrationClusters.size() == 0) {
            logger.info("[process][{}][no cluster]{}", event.getId(), migrationClusters);
            return;
        }

        processCluster(migrationClusters.get(0));
    }

    private boolean lockBeforeProcess() {
        if (!migrationLock.updateLock()) {
            logger.info("[lockBeforeProcess][{}] lock fail, skip", event.getId());
            return false;
        }
        if (!running.compareAndSet(false, true)) {
            logger.info("[lockBeforeProcess][{}] is running, skip", event.getId());
            return false;
        }

        return true;
    }

    private void unlockAfterProcess() {
        migrationLock.releaseLock();
        running.set(false);
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

    private MigrationCluster tryGetMigrationCluster(long clusterId) throws ClusterNotFoundException {
        MigrationCluster migrationCluster = getMigrationCluster(clusterId);
        if(migrationCluster == null){
            throw new ClusterNotFoundException(clusterId);
        }

        return migrationCluster;
    }

    private MigrationCluster tryGetMigrationCluster(String clusterName) throws ClusterNotFoundException {
        MigrationCluster migrationCluster = getMigrationCluster(clusterName);
        if(migrationCluster == null){
            throw new ClusterNotFoundException(clusterName);
        }

        return migrationCluster;
    }

    private void processCluster(MigrationCluster migrationCluster) {
        if (!lockBeforeProcess()) return;

        // every migration cluster only allow start one times
        migrationClusters.values().forEach(MigrationCluster::allowStart);
        tryStartClusterMigration(migrationCluster);
    }

    @Override
    public void processCluster(long clusterId) throws ClusterNotFoundException {
        MigrationCluster migrationCluster = tryGetMigrationCluster(clusterId);
        processCluster(migrationCluster);
    }

    @Override
    public void cancelCluster(long clusterId) throws ClusterNotFoundException {
        MigrationCluster migrationCluster = tryGetMigrationCluster(clusterId);
        if (!lockBeforeProcess()) return;

        try {
            migrationCluster.cancel();
        } catch (Throwable th) {
            logger.info("[cancelCluster][{}][{}] fail", event.getId(), clusterId, th);
            unlockAfterProcess();
        }
    }

    @Override
    public void forceClusterPublish(long clusterId) throws ClusterNotFoundException {
        MigrationCluster migrationCluster = tryGetMigrationCluster(clusterId);
        if (!lockBeforeProcess()) return;

        try {
            migrationCluster.forcePublish();
        } catch (Throwable th) {
            logger.info("[forceClusterPublish][{}][{}] fail", event.getId(), clusterId, th);
            unlockAfterProcess();
        }
    }

    @Override
    public void forceClusterEnd(long clusterId) throws ClusterNotFoundException {
        MigrationCluster migrationCluster = tryGetMigrationCluster(clusterId);
        if (!lockBeforeProcess()) return;

        try {
            migrationCluster.forceEnd();
        } catch (Throwable th) {
            logger.info("[forceClusterEnd][{}][{}] fail", event.getId(), clusterId, th);
            unlockAfterProcess();
        }
    }

    @Override
    public MigrationCluster rollbackCluster(long clusterId) throws ClusterNotFoundException {
        MigrationCluster migrationCluster = tryGetMigrationCluster(clusterId);
        if (!lockBeforeProcess()) return migrationCluster;

        return rollbackCluster(migrationCluster);
    }

    @Override
    public MigrationCluster rollbackCluster(String clusterName) throws ClusterNotFoundException {
        MigrationCluster migrationCluster = tryGetMigrationCluster(clusterName);
        if (!lockBeforeProcess()) return migrationCluster;

        return rollbackCluster(migrationCluster);
    }

    private MigrationCluster rollbackCluster(MigrationCluster migrationCluster) {
        try {
            migrationCluster.rollback();
        } catch (Throwable th) {
            logger.info("[rollbackCluster][{}][{}] fail", event.getId(), migrationCluster.clusterName(), th);
            unlockAfterProcess();
        }

        return migrationCluster;
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
            if (((MigrationCluster) args).getStatus().isTerminated()) {
                // Submit next task according to policy
                processNext();
            }
        }
        int finishedCnt = 0;
        int stopped = 0;
        int totalClusters = migrationClusters.size();
        for (MigrationCluster cluster : migrationClusters.values()) {
            if (cluster.getStatus().isTerminated()) {
                ++finishedCnt;
                continue;
            }
            if (!cluster.isStarted()) {
                ++stopped;
            }
        }
        if (finishedCnt == totalClusters) {
            // migration done
            notifyObservers(this);
        }

        if (finishedCnt + stopped >= totalClusters) {
            unlockAfterProcess();
        }
    }

    private void processNext() {

        for (MigrationCluster migrationCluster : migrationClusters.values()) {
            if (!migrationCluster.getStatus().isTerminated() && !migrationCluster.isStarted()) {
                tryStartClusterMigration(migrationCluster);
            }
        }
    }

    private void tryStartClusterMigration(MigrationCluster migrationCluster) {
        try {
            migrationCluster.start();
        } catch (Exception e) {
            logger.info("[process][{}] {} start fail", getMigrationEventId(), migrationCluster.clusterName(), e);
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
//            logger.info("[isDone][true]{}, success:{}", getMigrationEventId(), successCnt);
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format("[eventId:%d]", getMigrationEventId());
    }
}
