package com.ctrip.xpipe.redis.console.migration.model.impl;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.observer.AbstractObservable;
import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.migration.model.*;
import com.ctrip.xpipe.redis.console.migration.status.*;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.utils.VisibleForTesting;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author shyin
 *         <p>
 *         Dec 8, 2016
 */
public class DefaultMigrationCluster extends AbstractObservable implements MigrationCluster {

    private volatile MigrationState currentState;

    private MigrationEvent event;
    private MigrationClusterTbl migrationCluster;
    private List<MigrationShard> migrationShards = new LinkedList<>();

    private ClusterTbl currentCluster;
    private Map<Long, ShardTbl> shards;
    private Map<Long, DcTbl> dcs;

    private ClusterService clusterService;
    private ShardService shardService;
    private DcService dcService;
    private RedisService redisService;
    private MigrationService migrationService;

    private Executor executors;
    private ScheduledExecutorService scheduled;

    private OuterClientService outerClientService = OuterClientService.DEFAULT;

    public DefaultMigrationCluster(Executor executors, ScheduledExecutorService scheduled, MigrationEvent event, MigrationClusterTbl migrationCluster, DcService dcService, ClusterService clusterService, ShardService shardService,
                                   RedisService redisService, MigrationService migrationService) {
        this.event = event;
        this.migrationCluster = migrationCluster;
        this.clusterService = clusterService;
        this.shardService = shardService;
        this.dcService = dcService;
        this.redisService = redisService;
        this.migrationService = migrationService;
        this.executors = executors;
        this.scheduled = scheduled;
        loadMetaInfo();
        setStatus();
    }

    public ScheduledExecutorService getScheduled() {
        return scheduled;
    }

    @Override
    public Executor getMigrationExecutor() {
        return executors;
    }

    @Override
    public MigrationEvent getMigrationEvent() {
        return this.event;
    }

    @Override
    public String fromDc() {

        long fromDcId = migrationCluster.getSourceDcId();
        return dcs.get(fromDcId).getDcName();
    }

    @Override
    public String destDc() {
        long destDcId = migrationCluster.getDestinationDcId();
        return dcs.get(destDcId).getDcName();
    }

    @Override
    public long fromDcId() {
        return migrationCluster.getSourceDcId();
    }

    @Override
    public long destDcId() {
        return migrationCluster.getDestinationDcId();
    }

    @Override
    public MigrationStatus getStatus() {
        return currentState.getStatus();
    }

    @Override
    public MigrationClusterTbl getMigrationCluster() {
        return migrationCluster;
    }

    @Override
    public List<MigrationShard> getMigrationShards() {
        return migrationShards;
    }

    @Override
    public ClusterTbl getCurrentCluster() {
        return currentCluster;
    }

    @Override
    public Map<Long, ShardTbl> getClusterShards() {
        return shards;
    }

    @Override
    public Map<Long, DcTbl> getClusterDcs() {
        return dcs;
    }

    @Override
    public void addNewMigrationShard(MigrationShard migrationShard) {
        migrationShards.add(migrationShard);
    }

    @Override
    public void process() {
        logger.info("[process]{}-{}, {}", migrationCluster.getMigrationEventId(), clusterName(), this.currentState.getStatus());
        this.currentState.getStateActionState().tryAction();
    }

    @Override
    @DalTransaction
    public void updateStat(MigrationState stat) {

        logger.info("[updateStat]{}-{}, {} -> {}",
                migrationCluster.getMigrationEventId(), clusterName(), this.currentState.getStatus(), stat.getStatus());
        this.currentState = stat; // update local state even if update db fail
        this.getMigrationService().updateMigrationStatus(this, stat.getStatus());
        this.currentState = stat; // avoid local state updating by other thread before real update db with row lock
    }

    @Override
    public void updatePublishInfo(String desc) {
        migrationService.updatePublishInfoById(migrationCluster.getId(), desc);
    }

    @Override
    public void updateActiveDcIdToDestDcId() {

        long destDcId = destDcId();
        ClusterTbl cluster = getCurrentCluster();
        cluster.setActivedcId(destDcId);
        clusterService.updateActivedcId(clusterId(), destDcId);

    }

    @Override
    public ClusterStepResult stepStatus(ShardMigrationStep shardMigrationStep) {
        int finishCount = 0;
        int successCount = 0;

        List<MigrationShard> migrationShards = getMigrationShards();
        int shardSize = migrationShards.size();

        for(MigrationShard migrationShard : migrationShards){

            ShardMigrationStepResult shardMigrationStepResult = migrationShard.stepResult(shardMigrationStep);
            switch (shardMigrationStepResult){
                case FAIL:
                    finishCount++;
                    break;
                case SUCCESS:
                    finishCount++;
                    successCount++;
                    break;
                case UNKNOWN:
                    break;
                default:
                    throw new IllegalStateException("unkonw result:" + shardMigrationStep + "," + this);
            }
        }

        return new ClusterStepResult(shardSize, finishCount, successCount);
    }

    @Override
    public void markCheckFail(String failMessage) {

        logger.info("[markCheckFail]{}", clusterName());

        for(MigrationShard migrationShard : getMigrationShards()){
            migrationShard.markCheckFail(failMessage);
        }

    }

    @Override
    public OuterClientService getOuterClientService() {

        return outerClientService;
    }

    //for unit test
    public void setOuterClientService(OuterClientService outerClientService) {
        this.outerClientService = outerClientService;
    }

    private long clusterId() {
        return getCurrentCluster().getId();
    }

    @Override
    public String clusterName() {
        return getCurrentCluster().getClusterName();
    }

    @Override
    public void cancel() {
        logger.info("[Cancel]{}-{}, {} -> Cancelled", migrationCluster.getMigrationEventId(), clusterName(), this.currentState.getStatus());
        this.currentState.getStateActionState().tryRollback();
    }

    @Override
    public void rollback() {
        logger.info("[Rollback]{}-{}, {} -> Rollback", migrationCluster.getMigrationEventId(), clusterName(), this.currentState.getStatus());
        this.currentState.getStateActionState().tryRollback();
    }

    @Override
    public void forceProcess() {
        logger.info("[ForceProcess]{}-{}, {} -> ForceProcess", migrationCluster.getMigrationEventId(), clusterName(), this.currentState.getStatus());
        if (!(currentState instanceof ForceProcessAbleState)) {
            throw new IllegalStateException(String.format("cannot cancel while %s", this.currentState.getStatus()));
        }
        ForceProcessAbleState forceProcessAbleState = (ForceProcessAbleState) this.currentState;
        forceProcessAbleState.updateAndForceProcess();
    }

    @Override
    public void forceEnd() {
        logger.info("[ForceEnd]{}-{}, {} -> ForceEnd", migrationCluster.getMigrationEventId(), clusterName(), this.currentState.getStatus());
        if (!(currentState instanceof PublishState)) {
            throw new IllegalStateException(String.format("Cannot force end while %s", this.currentState.getStatus()));
        }
        PublishState publishState = (PublishState) this.currentState;
        publishState.forceEnd();
    }

    @Override
    public void update(Object args, Observable observable) {

        logger.info("[update]{}", args);
        this.currentState.refresh();
        notifyObservers(this);
    }

    @Override
    public ClusterService getClusterService() {
        return clusterService;
    }

    @Override
    public ShardService getShardService() {
        return shardService;
    }

    @Override
    public DcService getDcService() {
        return dcService;
    }

    @Override
    public RedisService getRedisService() {
        return redisService;
    }

    @Override
    public MigrationService getMigrationService() {
        return migrationService;
    }

    private void setStatus() {

        MigrationStatus status = MigrationStatus.valueOf(migrationCluster.getStatus());
        currentState = status.createMigrationState(this);
    }

    private void loadMetaInfo() {
        this.currentCluster = getClusterService().find(migrationCluster.getClusterId());
        this.shards = generateShardMap(getShardService().findAllByClusterName(currentCluster.getClusterName()));
        this.dcs = generateDcMap(getDcService().findClusterRelatedDc(currentCluster.getClusterName()));
    }

    private Map<Long, ShardTbl> generateShardMap(List<ShardTbl> shards) {
        Map<Long, ShardTbl> result = new HashMap<>();

        for (ShardTbl shard : shards) {
            result.put(shard.getId(), shard);
        }

        return result;
    }

    private Map<Long, DcTbl> generateDcMap(List<DcTbl> dcs) {

        Map<Long, DcTbl> result = new HashMap<>();

        for (DcTbl dc : dcs) {
            result.put(dc.getId(), dc);
        }

        return result;
    }

    @Override
    public String toString() {
        return String.format("[cluster:%s, state:%s]", clusterName(), currentState);
    }

    @VisibleForTesting
    public void setMigrationState(MigrationState state) {
        this.currentState = state;
    }

    @VisibleForTesting
    public void setMigrationExecutors(Executor executors) {
        this.executors = executors;
    }
}
