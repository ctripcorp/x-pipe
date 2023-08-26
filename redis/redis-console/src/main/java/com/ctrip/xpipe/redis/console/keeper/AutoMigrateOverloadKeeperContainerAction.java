package com.ctrip.xpipe.redis.console.keeper;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.console.AbstractCrossDcIntervalAction;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.service.model.ShardModelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

        Set<DcClusterShard> alreadyMigrateShards = new HashSet<>();
        for (MigrationKeeperContainerDetailModel migrationKeeperContainerDetailModel : readyToMigrationKeeperContainers) {
            List<DcClusterShard> migrateShards = migrationKeeperContainerDetailModel.getMigrateShards();
            if (CollectionUtils.isEmpty(migrateShards)) continue;

            String srcKeeperContainerIp = migrationKeeperContainerDetailModel.getSrcKeeperContainer().getKeeperIp();
            for (DcClusterShard migrateShard : migrateShards) {
                ShardModel shardModel = shardModelService.getShardModel(migrateShard.getDcId(),
                        migrateShard.getClusterId(), migrateShard.getShardId(), false, null);
                if (!alreadyMigrateShards.add(migrateShard)) continue;
                shardModelService.migrateShardKeepers(migrateShard.getDcId(), migrateShard.getClusterId(), shardModel,
                        migrationKeeperContainerDetailModel.getTargetKeeperContainer().getKeeperIp(), srcKeeperContainerIp);
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
