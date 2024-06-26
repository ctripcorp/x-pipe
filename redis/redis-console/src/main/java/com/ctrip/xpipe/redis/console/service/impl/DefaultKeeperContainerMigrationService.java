package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.service.KeeperContainerMigrationService;
import com.ctrip.xpipe.redis.console.service.model.ShardModelService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ctrip.xpipe.redis.console.keeper.AutoMigrateOverloadKeeperContainerAction.*;

@Component
public class DefaultKeeperContainerMigrationService implements KeeperContainerMigrationService {

    private Logger logger = LoggerFactory.getLogger(DefaultKeeperContainerMigrationService.class);

    @Autowired
    private ShardModelService shardModelService;

    private volatile List<MigrationKeeperContainerDetailModel> readyToMigrationKeeperContainers;

    private volatile AtomicBoolean isBegin = new AtomicBoolean(false);

    @Override
    public boolean beginMigrateKeeperContainers(List<MigrationKeeperContainerDetailModel> keeperContainerDetailModels) throws Throwable {
        if (!isBegin.compareAndSet(false, true)) {
            throw new Throwable("Migration tasks have already begin!!");
        }
        try {
            readyToMigrationKeeperContainers = keeperContainerDetailModels;
            for (MigrationKeeperContainerDetailModel keeperContainer : readyToMigrationKeeperContainers) {
                if(!isBegin.get()) break;
                List<DcClusterShard> migrateShards = keeperContainer.getMigrateShards();
                if (CollectionUtils.isEmpty(migrateShards)) continue;

                String srcKeeperContainerIp = keeperContainer.getSrcKeeperContainer().getKeeperIp();
                String targetKeeperContainerIp = keeperContainer.getTargetKeeperContainer().getKeeperIp();
                for (DcClusterShard migrateShard : migrateShards) {
                    if(!isBegin.get()) break;
                    keeperContainer.setMigratingShard(migrateShard);
                    ShardModel shardModel = shardModelService.getShardModel(migrateShard.getDcId(),
                            migrateShard.getClusterId(), migrateShard.getShardId(), false, null);
                    logger.info("[migrateKeeperContainers][begin][{}-{}-{}][{}->{}]",
                            migrateShard.getDcId(), migrateShard.getClusterId(), migrateShard.getShardId(), srcKeeperContainerIp, targetKeeperContainerIp);
                    String event;
                    if (keeperContainer.isSwitchActive()) {
                        if (shardModelService.switchActiveKeeper(srcKeeperContainerIp, targetKeeperContainerIp, shardModel)) {
                            keeperContainer.migrateKeeperCompleteCountIncrease();
                            event = KEEPER_SWITCH_MASTER_SUCCESS;
                            keeperContainer.addFinishedShard(migrateShard);
                        } else {
                            event = KEEPER_SWITCH_MASTER_FAIL;
                            keeperContainer.addFailedShard(migrateShard);
                        }
                    }else if (keeperContainer.isKeeperPairOverload()) {
                        if (shardModelService.migrateBackupKeeper(migrateShard.getDcId(), migrateShard.getClusterId(), shardModel,
                                srcKeeperContainerIp, targetKeeperContainerIp)) {
                            keeperContainer.migrateKeeperCompleteCountIncrease();
                            event = KEEPER_MIGRATION_BACKUP_SUCCESS;
                            keeperContainer.addFinishedShard(migrateShard);
                        } else {
                            event = KEEPER_MIGRATION_BACKUP_FAIL;
                            keeperContainer.addFailedShard(migrateShard);
                        }
                    }else {
                        try {
                            if (shardModelService.migrateActiveKeeper(migrateShard.getDcId(), migrateShard.getClusterId(), shardModel,
                                    srcKeeperContainerIp, targetKeeperContainerIp)) {
                                keeperContainer.migrateKeeperCompleteCountIncrease();
                                event = KEEPER_MIGRATION_ACTIVE_SUCCESS;
                                keeperContainer.addFinishedShard(migrateShard);
                            } else {
                                event = KEEPER_MIGRATION_ACTIVE_FAIL;
                                keeperContainer.addFailedShard(migrateShard);
                            }
                        } catch (Throwable th) {
                            event = KEEPER_MIGRATION_ACTIVE_ROLLBACK_ERROR;
                            keeperContainer.addFailedShard(migrateShard);
                        }
                    }
                    CatEventMonitor.DEFAULT.logEvent(KEEPER_MIGRATION, event);
                    logger.info("[migrateKeeperContainers][{}-{}-{}][{}->{}] {}",
                            migrateShard.getDcId(), migrateShard.getClusterId(), migrateShard.getShardId(), srcKeeperContainerIp, targetKeeperContainerIp, event);
                }
                keeperContainer.setMigratingShard(null);
            }
            return true;
        } finally {
            isBegin.set(false);
        }
    }

    @Override
    public List<MigrationKeeperContainerDetailModel> getMigrationProcess() {
        return readyToMigrationKeeperContainers;
    }

    @Override
    public boolean stopMigrate() {
        return isBegin.compareAndSet(true, false);
    }
}
