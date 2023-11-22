package com.ctrip.xpipe.redis.console.service.impl;

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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ctrip.xpipe.redis.console.keeper.AutoMigrateOverloadKeeperContainerAction.KEEPER_MIGRATION_FAIL;
import static com.ctrip.xpipe.redis.console.keeper.AutoMigrateOverloadKeeperContainerAction.KEEPER_MIGRATION_SUCCESS;

@Component
public class DefaultKeeperContainerMigrationService implements KeeperContainerMigrationService {

    private Logger logger = LoggerFactory.getLogger(DefaultKeeperContainerMigrationService.class);

    @Autowired
    private ShardModelService shardModelService;

    private volatile List<MigrationKeeperContainerDetailModel> readyToMigrationKeeperContainers;

    private volatile AtomicBoolean isBegin = new AtomicBoolean(false);

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
                ShardModel shardModel = shardModelService.getShardModel(migrateShard.getDcId(),
                        migrateShard.getClusterId(), migrateShard.getShardId(), false, null);
                if (!alreadyMigrateShards.add(migrateShard)) {
                    logger.info("[beginMigrateKeeperContainers] shard {} has already migrated, should not migrate in the same time", migrateShard);
                    continue;
                }
                logger.debug("[beginMigrateKeeperContainers] begin migrate shard {} from srcKeeperContainer:{} to targetKeeperContainer:{}",
                        migrateShard, srcKeeperContainerIp, keeperContainer.getTargetKeeperContainer().getKeeperIp());
                if (shardModelService.migrateShardKeepers(migrateShard.getDcId(), migrateShard.getClusterId(), shardModel,
                        srcKeeperContainerIp, keeperContainer.getTargetKeeperContainer().getKeeperIp())) {
                    keeperContainer.migrateKeeperCompleteCountIncrease();
                    CatEventMonitor.DEFAULT.logEvent(KEEPER_MIGRATION_SUCCESS, String.format("dc:%s, cluster:%s, shard:%s, src:%s, target:%s",
                            migrateShard.getDcId(), migrateShard.getClusterId(), migrateShard.getShardId(), srcKeeperContainerIp,
                            keeperContainer.getTargetKeeperContainer().getKeeperIp()));
                } else {
                    CatEventMonitor.DEFAULT.logEvent(KEEPER_MIGRATION_FAIL, String.format("dc:%s, cluster:%s, shard:%s, src:%s, target:%s",
                            migrateShard.getDcId(), migrateShard.getClusterId(), migrateShard.getShardId(), srcKeeperContainerIp,
                            keeperContainer.getTargetKeeperContainer().getKeeperIp()));
                }
            }
        }
        isBegin.set(false);
    }

    @Override
    public List<MigrationKeeperContainerDetailModel> getMigrationProcess() {
        return readyToMigrationKeeperContainers;
    }
}
