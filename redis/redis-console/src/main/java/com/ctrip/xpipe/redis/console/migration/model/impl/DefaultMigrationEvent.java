package com.ctrip.xpipe.redis.console.migration.model.impl;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.observer.AbstractObservable;
import com.ctrip.xpipe.redis.console.migration.exception.MigrationUnderProcessingException;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.model.MigrationLock;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.MigrationEventTbl;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterNotFoundException;
import com.ctrip.xpipe.utils.VisibleForTesting;
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
    public void process() throws Exception {
        List<MigrationCluster> localMigrationClusters = getMigrationClusters();
        if (localMigrationClusters.isEmpty()) {
            logger.info("[process][{}][no cluster]{}", event.getId(), localMigrationClusters);
            return;
        }

        processCluster(localMigrationClusters.get(0));
    }

    private boolean lockBeforeProcess() {
        if (!running.compareAndSet(false, true)) {
            logger.info("[lockBeforeProcess][{}] is running, skip", event.getId());
            return false;
        }
        try {
            migrationLock.updateLock();
        } catch (Throwable th) {
            logger.info("[lockBeforeProcess][{}] lock fail, skip", event.getId(), th);
            running.set(false);
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

    private void allowAllClustersStart() {
        migrationClusters.values().forEach(cluster -> cluster.allowStart(true));
    }

    private void allowOneClusterStart(long clusterId) {
        migrationClusters.forEach((id, cluster) -> cluster.allowStart(id.equals(clusterId)));
    }

    private void processCluster(MigrationCluster migrationCluster) throws Exception {
        if (!lockBeforeProcess()) throw new MigrationUnderProcessingException(getMigrationEventId());

        allowAllClustersStart();
        try {
            migrationCluster.start();
        } catch (Exception e) {
            logger.info("[processCluster][{}] {} start fail", getMigrationEventId(), migrationCluster.clusterName(), e);
            unlockAfterProcess();
            throw e;
        }
    }

    @Override
    public void processCluster(long clusterId) throws Exception {
        MigrationCluster migrationCluster = tryGetMigrationCluster(clusterId);
        processCluster(migrationCluster);
    }

    @Override
    public void cancelCluster(long clusterId) throws ClusterNotFoundException {
        MigrationCluster migrationCluster = tryGetMigrationCluster(clusterId);
        if (!lockBeforeProcess()) return;

        try {
            allowOneClusterStart(clusterId);
            migrationCluster.cancel();
        } catch (Throwable th) {
            logger.info("[cancelCluster][{}][{}] fail", event.getId(), clusterId, th);
            unlockAfterProcess();
        }
    }

    @Override
    public void forceClusterProcess(long clusterId) throws ClusterNotFoundException {
        MigrationCluster migrationCluster = tryGetMigrationCluster(clusterId);
        if (!lockBeforeProcess()) return;

        try {
            allowOneClusterStart(clusterId);
            migrationCluster.forceProcess();
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
            allowOneClusterStart(clusterId);
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

        allowOneClusterStart(clusterId);
        return rollbackCluster(migrationCluster);
    }

    @Override
    public MigrationCluster rollbackCluster(String clusterName) throws ClusterNotFoundException {
        MigrationCluster migrationCluster = tryGetMigrationCluster(clusterName);
        if (!lockBeforeProcess()) return migrationCluster;

        allowOneClusterStart(migrationCluster.getMigrationCluster().getClusterId());
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
            if (((MigrationCluster) args).getStatus().isTerminated() || !((MigrationCluster) args).isStarted()) {
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
            if (!migrationCluster.getStatus().isTerminated()
                    && !MigrationStatus.RollBack.equals(migrationCluster.getStatus())
                    && !migrationCluster.isStarted()) {
                try {
                    migrationCluster.start();
                } catch (Exception e) {
                    logger.info("[processNext][{}] {} start fail", getMigrationEventId(), migrationCluster.clusterName(), e);
                }
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
//            logger.info("[isDone][true]{}, success:{}", getMigrationEventId(), successCnt);
            return true;
        }
        return false;
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @VisibleForTesting
    public void setMigrationLock(MigrationLock lock) {
        this.migrationLock = lock;
    }

    @VisibleForTesting
    public Map<Long, MigrationCluster> getMigrationClustersMap() {
        return migrationClusters;
    }

    @VisibleForTesting
    public void setMigrationClustersMap(Map<Long, MigrationCluster> migrationClusters) {
        this.migrationClusters = migrationClusters;
    }

    @Override
    public String toString() {
        return String.format("[eventId:%d]", getMigrationEventId());
    }
}
