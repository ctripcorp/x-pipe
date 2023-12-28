package com.ctrip.xpipe.redis.console.keeper;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.console.AbstractCrossDcIntervalAction;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.service.model.ShardModelService;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.HashSet;
import java.util.Iterator;
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

    @Autowired
    private AlertManager alertManager;

    private final List<ALERT_TYPE> alertType = Lists.newArrayList(ALERT_TYPE.KEEPER_MIGRATION_FAIL, ALERT_TYPE.KEEPER_MIGRATION_SUCCESS);

    public final static String KEEPER_MIGRATION_SUCCESS = "keeper_migration_success";

    public final static String KEEPER_MIGRATION_FAIL = "keeper_migration_fail";

    @Override
    protected void doAction() {
        List<MigrationKeeperContainerDetailModel> readyToMigrationKeeperContainers = analyzer.getCurrentDcReadyToMigrationKeeperContainers();
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
            Iterator<DcClusterShard> iterator= migrateShards.iterator();
            while (iterator.hasNext()) {
                DcClusterShard migrateShard = iterator.next();
                if (!alreadyMigrateShards.add(migrateShard)) {
                    logger.info("[migrateAllKeepers] shard {} has already migrated, should not migrate in the same time", migrateShard);
                    continue;
                }

                ShardModel shardModel = shardModelService.getShardModel(migrateShard.getDcId(),
                        migrateShard.getClusterId(), migrateShard.getShardId(), false, null);

                if (!shardModelService.migrateShardKeepers(migrateShard.getDcId(), migrateShard.getClusterId(), shardModel,
                        migrationKeeperContainerDetailModel.getTargetKeeperContainer().getKeeperIp(), srcKeeperContainerIp)) {
                    logger.warn("[migrateAllKeepers] migrate shard keepers failed, shard: {}", migrateShard);
                    alertForKeeperMigrationFail(migrateShard, srcKeeperContainerIp,
                            migrationKeeperContainerDetailModel.getTargetKeeperContainer().getKeeperIp());
                    continue;
                }

                alertForKeeperMigrationSuccess(migrateShard, srcKeeperContainerIp,
                        migrationKeeperContainerDetailModel.getTargetKeeperContainer().getKeeperIp());
                migrationKeeperContainerDetailModel.migrateKeeperCompleteCountIncrease();
                iterator.remove();
            }
        }
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return alertType;
    }

    private void alertForKeeperMigrationSuccess(DcClusterShard dcClusterShard, String srcKeeperContainerIp, String targetKeeperContainerIp) {
        CatEventMonitor.DEFAULT.logEvent(KEEPER_MIGRATION_SUCCESS, String.format("dc:%s, cluster:%s, shard:%s, src:%s, target:%s",
                dcClusterShard.getDcId(), dcClusterShard.getClusterId(), dcClusterShard.getShardId(), srcKeeperContainerIp,
                targetKeeperContainerIp));

        alertManager.alert(dcClusterShard.getDcId(), dcClusterShard.getClusterId(), dcClusterShard.getShardId(),
                new HostPort(srcKeeperContainerIp, 0), ALERT_TYPE.KEEPER_MIGRATION_SUCCESS, "keeper migration success");
    }

    private void alertForKeeperMigrationFail(DcClusterShard dcClusterShard, String srcKeeperContainerIp, String targetKeeperContainerIp) {
        CatEventMonitor.DEFAULT.logEvent(KEEPER_MIGRATION_FAIL, String.format("dc:%s, cluster:%s, shard:%s, src:%s, target:%s",
                dcClusterShard.getDcId(), dcClusterShard.getClusterId(), dcClusterShard.getShardId(), srcKeeperContainerIp,
                targetKeeperContainerIp));

        alertManager.alert(dcClusterShard.getDcId(), dcClusterShard.getClusterId(), dcClusterShard.getShardId(),
                new HostPort(srcKeeperContainerIp, 0), ALERT_TYPE.KEEPER_MIGRATION_FAIL, "keeper migration fail");
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
