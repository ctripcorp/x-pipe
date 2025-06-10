package com.ctrip.xpipe.redis.checker.config.impl;

import com.ctrip.xpipe.api.config.ConfigProvider;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import java.util.Set;

@Configuration
public class ConsoleConfigBean extends AbstractConfigBean {

    public static final String KEY_CONFIG_DEFAULT_RESTORE_HOUR = "console.config.default.restore.hour";

    public static final String KEY_CLUSTER_SHARD_FOR_MIGRATE_SYS_CHECK = "console.cluster.shard.for.migrate.sys.check";

    public static final String KEY_NO_HEALTH_CHECK_MINUTES = "no.health.check.minutes";

    public static final String KEY_REBALANCE_SENTINEL_MAX_NUM_ONCE = "rebalance.sentinel.max.num.once";

    public static final String KEY_REBALANCE_SENTINEL_INTERVAL = "rebalance.sentinel.interval.second";

    public static final String KEY_CONSOLE_NOTIFY_RETRY_INTERVAL = "console.notify.retry.interval";

    public static final String KEY_CONSOLE_NOTIFY_RETRY_TIMES = "console.notify.retry.times";

    public static final String KEY_CONSOLE_NOTIFY_THREADS = "console.notify.threads";

    public static final String KEY_CACHE_REFERSH_INTERVAL = "console.cache.refresh.interval";

    public static final String KEY_MIGRATION_TIMEOUT_MILLI = "migration.timeout.milli";

    public static final String KEY_SERVLET_METHOD_TIMEOUT_MILLI = "servlet.method.timeout.milli";

    public static final String KEY_CONSOLE_KEEPER_PAIR_OVERLOAD_FACTOR = "console.keeper.container.pair.overload.standard.factor";

    public static final String KEY_CONSOLE_KEEPER_CONTAINER_DISK_OVERLOAD_FACTOR = "console.keeper.container.disk.overload.factor";

    public static final String KEY_CONSOLE_KEEPER_CONTAINER_IO_RATE = "console.keeper.container.io.rate";

    public static final String KEY_CONSOLE_AUTO_MIGRATE_OVERLOAD_KEEPER_CONTAINER_OPEN = "console.auto.migrate.overload.keeper.container.open";

    public static final String KEY_CONSOLE_AUTO_MIGRATE_OVERLOAD_KEEPER_CONTAINER_INTERVAL_MILLI = "console.auto.migrate.overload.keeper.container.interval.milli";

    public static final String KEY_OWN_CLUSTER_TYPES = "console.cluster.types";

    public static final String KEY_OUTER_CLUSTER_TYPES = "console.outer.cluster.types";

    public static final String KEY_OUTER_CLIENT_SYNC_INTERVAL = "console.outer.client.sync.interval";

    public static final String KEY_PROXY_INFO_CHECK_INTERVAL = "console.proxy.info.collector.check.interval";

    public static final String KEY_VARIABLES_CHECK_DATASOURCE = "console.health.variables.datasource";

    public static final String KEY_CONSOLE_META_SLOT_CACHE_MILLI = "console.meta.slot.cache.milli";

    private static final String KEY_KEEPERCONTAINER_SYNC_LIMIT_ON = "keepercontainer.sync.limit.on";

    private FoundationService foundationService;

    @Autowired
    public ConsoleConfigBean(FoundationService foundationService) {
        super(ConfigProvider.DEFAULT.getOrCreateConfig(ConfigProvider.CONSOLE_CONFIG_NAME));
        this.foundationService = foundationService;
    }

    public int getConfigDefaultRestoreHours() {
        return getIntProperty(KEY_CONFIG_DEFAULT_RESTORE_HOUR, 10);
    }

    public Pair<String, String> getClusterShardForMigrationSysCheck() {
        String clusterShard = getProperty(KEY_CLUSTER_SHARD_FOR_MIGRATE_SYS_CHECK, "cluster1, shard1");
        String[] strs = StringUtil.splitRemoveEmpty("\\s*,\\s*", clusterShard);
        return Pair.from(strs[0], strs[1]);
    }

    public int getHealthCheckSuspendMinutes() {
        return getIntProperty(KEY_NO_HEALTH_CHECK_MINUTES, 40);
    }

    public int getRebalanceSentinelMaxNumOnce() {
        return getIntProperty(KEY_REBALANCE_SENTINEL_MAX_NUM_ONCE, 15);
    }

    public int getRebalanceSentinelInterval() {
        return getIntProperty(KEY_REBALANCE_SENTINEL_INTERVAL, 120);
    }

    public int getConsoleNotifyRetryInterval() {
        return getIntProperty(KEY_CONSOLE_NOTIFY_RETRY_INTERVAL, 100);
    }

    public int getConsoleNotifyRetryTimes() {
        return getIntProperty(KEY_CONSOLE_NOTIFY_RETRY_TIMES, 3);
    }

    public int getConsoleNotifyThreads() {
        return getIntProperty(KEY_CONSOLE_NOTIFY_THREADS, 20);
    }

    public int getCacheRefreshInterval() {
        return getIntProperty(KEY_CACHE_REFERSH_INTERVAL, 1000);
    }

    public long getMigrationTimeoutMilli() {
        return getLongProperty(KEY_MIGRATION_TIMEOUT_MILLI, 15000L);
    }

    public long getServletMethodTimeoutMilli() {
        return getLongProperty(KEY_SERVLET_METHOD_TIMEOUT_MILLI, 10000L);
    }

    public double getKeeperPairOverLoadFactor() {
        return getFloatProperty(KEY_CONSOLE_KEEPER_PAIR_OVERLOAD_FACTOR, 0.25F);
    }

    public double getKeeperContainerDiskOverLoadFactor() {
        return getFloatProperty(KEY_CONSOLE_KEEPER_CONTAINER_DISK_OVERLOAD_FACTOR, 0.8F);
    }

    public double getKeeperContainerIoRate() {
        return getFloatProperty(KEY_CONSOLE_KEEPER_CONTAINER_IO_RATE, 500F);
    }

    public boolean isAutoMigrateOverloadKeeperContainerOpen() {
        return getBooleanProperty(KEY_CONSOLE_AUTO_MIGRATE_OVERLOAD_KEEPER_CONTAINER_OPEN, false);
    }

    public long getAutoMigrateOverloadKeeperContainerIntervalMilli() {
        return getLongProperty(KEY_CONSOLE_AUTO_MIGRATE_OVERLOAD_KEEPER_CONTAINER_INTERVAL_MILLI, 60 * 60 * 1000L);
    }

    public Set<String> getOwnClusterType() {
//        String clusterTypes = getProperty(KEY_OWN_CLUSTER_TYPES, ClusterType.ONE_WAY.toString());
        String clusterTypes = getProperty(KEY_OWN_CLUSTER_TYPES, "ONE_WAY,BI_DIRECTION,SINGLE_DC,LOCAL_DC,CROSS_DC,HETERO");

        return getSplitStringSet(clusterTypes);
    }

    public Set<String> getOuterClusterTypes() {
        String clusterTypes = getProperty(KEY_OUTER_CLUSTER_TYPES, "");

        return getSplitStringSet(clusterTypes);
    }

    public int getOuterClientSyncInterval() {
        return getIntProperty(KEY_OUTER_CLIENT_SYNC_INTERVAL, 10 * 1000);
    }

    public int getProxyInfoCollectInterval() {
        return getIntProperty(KEY_PROXY_INFO_CHECK_INTERVAL, 30 * 1000);
    }

    public Set<String> getVariablesCheckDataSources() {
        String dataSources = getProperty(KEY_VARIABLES_CHECK_DATASOURCE, "");
        return getSplitStringSet(dataSources);
    }

    public long getMetaServerSlotClusterMapCacheTimeOutMilli() {
        return getLongProperty(KEY_CONSOLE_META_SLOT_CACHE_MILLI, 30 * 1000L);
    }

    public boolean autoSetKeeperSyncLimit() {
        return getBooleanProperty(KEY_KEEPERCONTAINER_SYNC_LIMIT_ON, false);
    }

}
