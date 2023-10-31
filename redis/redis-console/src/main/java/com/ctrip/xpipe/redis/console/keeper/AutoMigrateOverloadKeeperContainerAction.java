package com.ctrip.xpipe.redis.console.keeper;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.console.AbstractCrossDcIntervalAction;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.service.model.ShardModelService;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author yu
 * <p>
 * 2023/9/27
 */
@Component
public class AutoMigrateOverloadKeeperContainerAction extends AbstractCrossDcIntervalAction {

    @Autowired
    private KeeperContainerUsedInfoAnalyzer analyzer;

    @Autowired
    private ShardModelService shardModelService;

    private final List<ALERT_TYPE> alertType = Collections.emptyList();

    @Override
    protected void doAction() {
        List<MigrationKeeperContainerDetailModel> readyToMigrationKeeperContainers = analyzer.getAllDcReadyToMigrationKeeperContainers();
        if (CollectionUtils.isEmpty(readyToMigrationKeeperContainers)) return;

        migrateAllKeepers(readyToMigrationKeeperContainers);
    }

    @VisibleForTesting
    void migrateAllKeepers(List<MigrationKeeperContainerDetailModel> readyToMigrationKeeperContainers) {
        Set<DcClusterShard> alreadyMigrateShards = new HashSet<>();
        for (MigrationKeeperContainerDetailModel migrationKeeperContainerDetailModel : readyToMigrationKeeperContainers) {
            List<DcClusterShard> migrateShards = migrationKeeperContainerDetailModel.getMigrateShards();
            if (CollectionUtils.isEmpty(migrateShards)) continue;

            String srcKeeperContainerIp = migrationKeeperContainerDetailModel.getSrcKeeperContainer().getKeeperIp();
            for (DcClusterShard migrateShard : migrateShards) {
                if (!alreadyMigrateShards.add(migrateShard)) continue;

                ShardModel shardModel = shardModelService.getShardModel(migrateShard.getDcId(),
                        migrateShard.getClusterId(), migrateShard.getShardId(), false, null);

                if (!shardModelService.migrateShardKeepers(migrateShard.getDcId(), migrateShard.getClusterId(), shardModel,
                        migrationKeeperContainerDetailModel.getTargetKeeperContainer().getKeeperIp(), srcKeeperContainerIp)) {
                    logger.warn("[migrateAllKeepers] migrate shard keepers failed, shard: {}", migrateShard);
                    continue;
                }
                // TODO song_yu 删除已迁移的分片, 该定时任务时间调整为10min
                // TODO song_yu  迁移keeper增加cat打点
                // TODO 增加alertType 发邮件出来
//                migrationKeeperContainerDetailModel.getMigrateShards().remove();
                migrationKeeperContainerDetailModel.migrateKeeperCompleteCountIncrease();

            }
        }
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return alertType;
    }

    @Override
    protected boolean shouldDoAction() {
        return consoleConfig.isAutoMigrateOverloadKeeperContainerOpen() && super.shouldDoAction();
    }

    @Override
    protected long getLeastIntervalMilli() {
        return consoleConfig.getAutoMigrateOverloadKeeperContainerIntervalMilli();
    }

    @Override
    protected long getIntervalMilli() {
        return consoleConfig.getAutoMigrateOverloadKeeperContainerIntervalMilli();
    }
}
