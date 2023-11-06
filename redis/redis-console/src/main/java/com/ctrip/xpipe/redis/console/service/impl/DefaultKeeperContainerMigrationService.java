package com.ctrip.xpipe.redis.console.service.impl;

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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class DefaultKeeperContainerMigrationService implements KeeperContainerMigrationService {

    private Logger logger = LoggerFactory.getLogger(DefaultKeeperContainerMigrationService.class);

    @Autowired
    private ShardModelService shardModelService;

    private volatile List<MigrationKeeperContainerDetailModel> readyToMigrationKeeperContainers;

    private volatile AtomicBoolean isBegin = new AtomicBoolean(false);

    private volatile AtomicBoolean isStop = new AtomicBoolean(false);

    @Override
    public void beginMigrateKeeperContainers(List<MigrationKeeperContainerDetailModel> keeperContainerDetailModels) {
        if (!isBegin.compareAndSet(false, true)) {
            logger.info("[beginMigrateKeeperContainers] has already begin!!");
            return;
        }
        logger.debug("[beginMigrateKeeperContainers] begin migrate keeper containers {}", keeperContainerDetailModels);
        readyToMigrationKeeperContainers = keeperContainerDetailModels;
        Set<DcClusterShard> alreadyMigrateShards = new HashSet<>();
        for (MigrationKeeperContainerDetailModel keeperContainer : readyToMigrationKeeperContainers) {
            List<DcClusterShard> migrateShards = keeperContainer.getMigrateShards();
            if (CollectionUtils.isEmpty(migrateShards)) continue;


            String srcKeeperContainerIp = keeperContainer.getSrcKeeperContainer().getKeeperIp();
            for (DcClusterShard migrateShard : migrateShards) {
                if (isStop.get() == true) {
                    logger.info("[beginMigrateKeeperContainers] stop migrating");
                    break;
                }
                ShardModel shardModel = shardModelService.getShardModel(migrateShard.getDcId(),
                        migrateShard.getClusterId(), migrateShard.getShardId(), false, null);
                if (!alreadyMigrateShards.add(migrateShard)) {
                    logger.info("[beginMigrateKeeperContainers] shard {} has already migrated, should not migrate in the same time", migrateShard);
                    continue;
                }
                logger.debug("[beginMigrateKeeperContainers] begin migrate shard {} from srcKeeperContainer:{} to targetKeeperContainer:{}",
                        migrateShard, srcKeeperContainerIp, keeperContainer.getTargetKeeperContainer().getKeeperIp());
                if (shardModelService.migrateShardKeepers(migrateShard.getDcId(), migrateShard.getClusterId(), shardModel,
                        srcKeeperContainerIp, keeperContainer.getTargetKeeperContainer().getKeeperIp()))
                    keeperContainer.migrateKeeperCompleteCountIncrease();
            }
        }
        isBegin.set(false);
        isStop.set(false);
    }

    @Override
    public void stopMigrateKeeperContainers() {
        isStop.compareAndSet(false, true);
    }

    @Override
    public List<MigrationKeeperContainerDetailModel> getMigrationProcess() {
        return readyToMigrationKeeperContainers;
    }
}
