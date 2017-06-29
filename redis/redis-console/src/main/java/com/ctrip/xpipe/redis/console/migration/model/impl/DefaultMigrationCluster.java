package com.ctrip.xpipe.redis.console.migration.model.impl;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.status.MigrationState;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.migration.status.PartialSuccessState;
import com.ctrip.xpipe.redis.console.migration.status.PublishState;
import com.ctrip.xpipe.redis.console.migration.status.migration.*;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.observer.AbstractObservable;
import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;

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
    private ExecutorService executors;

    public DefaultMigrationCluster(MigrationEvent event, MigrationClusterTbl migrationCluster, DcService dcService, ClusterService clusterService, ShardService shardService,
                                   RedisService redisService, MigrationService migrationService) {
        this.event = event;
        this.migrationCluster = migrationCluster;

        this.clusterService = clusterService;
        this.shardService = shardService;
        this.dcService = dcService;
        this.redisService = redisService;
        this.migrationService = migrationService;
        loadMetaInfo();
        executors = DefaultExecutorFactory.createAllowCoreTimeout(
                "Migration-" + clusterName(), shards.size() * 2
        ).createExecutorService();

        setStatus();

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
        logger.info("[Process]{}-{}, {}", migrationCluster.getMigrationEventId(), getCurrentCluster().getClusterName(), this.currentState.getStatus());
        this.currentState.action();
    }

    @Override
    @DalTransaction
    public void updateStat(MigrationState stat) {

        logger.info("[updateStat]{}-{}, {} -> {}",
                migrationCluster.getMigrationEventId(), clusterName(), this.currentState.getStatus(), stat.getStatus());

        this.currentState = stat;

        updateClusterStatus();

        updateMigrationClusterStatus();

    }

    private void updateMigrationClusterStatus() {

        MigrationStatus migrationStatus = this.currentState.getStatus();
        MigrationClusterTbl migrationCluster = getMigrationCluster();
        migrationCluster.setStatus(migrationStatus.toString());
        migrationCluster.setEndTime(new Date());
        getMigrationService().updateMigrationCluster(migrationCluster);
    }

    private void updateClusterStatus() {

        MigrationStatus migrationStatus = this.currentState.getStatus();
        String clusterStatus = migrationStatus.getClusterStatus().toString();
        ClusterTbl cluster = getCurrentCluster();
        cluster.setStatus(clusterStatus);
        logger.info("[updateStat][updatedb]{}, {}", clusterName(), clusterStatus);
        getClusterService().update(cluster);
        ClusterTbl newCluster = getClusterService().find(clusterName());
        logger.info("[updateStat][getdb]{}, {}", clusterName(), newCluster != null ? newCluster.getStatus() : null);
    }

    @Override
    public String clusterName() {
        return getCurrentCluster().getClusterName();
    }

    @Override
    public void cancel() {
        logger.info("[Cancel]{}-{}, {} -> Cancelled", migrationCluster.getMigrationEventId(), getCurrentCluster().getClusterName(), this.currentState.getStatus());
        this.currentState.rollback();
    }

    @Override
    public void rollback() {
        logger.info("[Rollback]{}-{}, {} -> Rollback", migrationCluster.getMigrationEventId(), getCurrentCluster().getClusterName(), this.currentState.getStatus());
        this.currentState.rollback();
    }

    @Override
    public void forcePublish() {
        logger.info("[ForcePublish]{}-{}, {} -> ForcePublish", migrationCluster.getMigrationEventId(), getCurrentCluster().getClusterName(), this.currentState.getStatus());
        if (!(currentState instanceof PartialSuccessState)) {
            throw new IllegalStateException(String.format("cannot cancel while %s", this.currentState.getStatus()));
        }
        PartialSuccessState partialSuccessState = (PartialSuccessState) this.currentState;
        partialSuccessState.forcePublish();
    }

    @Override
    public void forceEnd() {
        logger.info("[ForceEnd]{}-{}, {} -> ForceEnd", migrationCluster.getMigrationEventId(), getCurrentCluster().getClusterName(), this.currentState.getStatus());
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
}
