package com.ctrip.xpipe.redis.keeper.config;

import com.ctrip.xpipe.api.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class DefaultKeeperContainerConfig implements KeeperContainerConfig {

    private static final Logger logger = LoggerFactory.getLogger(DefaultKeeperContainerConfig.class);

    public static final String REPLICATION_STORE_DIR = "replication.store.dir";
    public static final String KEY_MODE = "mode";
    public static final String DISK_CHECK_INTERVAL_MILL = "disk.check.interval.mill";
    public static final String CHECK_ROUND = "health.check.round.before.mark.down";
    public static final String ELECTION_RESET_INTERVAL = "election.reset.interval.min";
    private Config config;

    @PostConstruct
    private void init() {
        config = Config.DEFAULT;
    }

    @Override
    public String getReplicationStoreDir() {
        return config.get(REPLICATION_STORE_DIR, getDefaultRdsDir());
    }

    @Override
    public String getMode() {
        String raw = config.get(KEY_MODE, MODE_NORMAL);
        if (raw == null || raw.trim().isEmpty()) {
            return MODE_NORMAL;
        }
        String mode = raw.trim();
        if (MODE_NORMAL.equalsIgnoreCase(mode)) {
            return MODE_NORMAL;
        }
        if (MODE_TFS.equalsIgnoreCase(mode)) {
            return MODE_TFS;
        }
        logger.error("[getMode] invalid mode={}, fallback to {}", mode, MODE_NORMAL);
        return MODE_NORMAL;
    }

    @Override
    public boolean isTfsMode() {
        return MODE_TFS.equals(getMode());
    }

    @Override
    public int diskCheckInterval() {
        return Integer.parseInt(config.get(DISK_CHECK_INTERVAL_MILL, "30000"));
    }

    @Override
    public int checkRoundBeforeMarkDown() {
        return Integer.parseInt(config.get(CHECK_ROUND, "3"));
    }

    @Override
    public int keeperLeaderResetMinInterval() {
        return Integer.parseInt(config.get(ELECTION_RESET_INTERVAL, "600"));
    }

    @Override
    public long ioHealthCheckIntervalMill() {
        return Long.parseLong(config.get(KEY_IO_HEALTH_CHECK_INTERVAL_MILL,
                String.valueOf(DEFAULT_IO_HEALTH_CHECK_INTERVAL_MILL)));
    }

    @Override
    public long ioHealthWarmupMill() {
        return Long.parseLong(config.get(KEY_IO_HEALTH_WARMUP_MILL,
                String.valueOf(DEFAULT_IO_HEALTH_WARMUP_MILL)));
    }

    @Override
    public int ioHealthMinRecords() {
        return Integer.parseInt(config.get(KEY_IO_HEALTH_MIN_RECORDS,
                String.valueOf(DEFAULT_IO_HEALTH_MIN_RECORDS)));
    }

    @Override
    public long ioHealthMetricIntervalMill() {
        return Long.parseLong(config.get(KEY_IO_HEALTH_METRIC_INTERVAL_MILL,
                String.valueOf(DEFAULT_IO_HEALTH_METRIC_INTERVAL_MILL)));
    }

    @Override
    public int ioHealthFailRatioPercent() {
        return Integer.parseInt(config.get(KEY_IO_HEALTH_FAIL_RATIO_PERCENT,
                String.valueOf(DEFAULT_IO_HEALTH_FAIL_RATIO_PERCENT)));
    }

    @Override
    public int ioHealthSlowRatioPercent() {
        return Integer.parseInt(config.get(KEY_IO_HEALTH_SLOW_RATIO_PERCENT,
                String.valueOf(DEFAULT_IO_HEALTH_SLOW_RATIO_PERCENT)));
    }

    @Override
    public int ioHealthTenantAbnormalPercent() {
        return Integer.parseInt(config.get(KEY_IO_HEALTH_TENANT_ABNORMAL_PERCENT,
                String.valueOf(DEFAULT_IO_HEALTH_TENANT_ABNORMAL_PERCENT)));
    }

    @Override
    public int ioHealthTenantAbnormalFloor() {
        return Integer.parseInt(config.get(KEY_IO_HEALTH_TENANT_ABNORMAL_FLOOR,
                String.valueOf(DEFAULT_IO_HEALTH_TENANT_ABNORMAL_FLOOR)));
    }

    @Override
    public int ioHealthFileConsecutiveFails() {
        return Integer.parseInt(config.get(KEY_IO_HEALTH_FILE_CONSECUTIVE_FAILS,
                String.valueOf(DEFAULT_IO_HEALTH_FILE_CONSECUTIVE_FAILS)));
    }

    @Override
    public int ioHealthFileFailsInWindow() {
        return Integer.parseInt(config.get(KEY_IO_HEALTH_FILE_FAILS_IN_WINDOW,
                String.valueOf(DEFAULT_IO_HEALTH_FILE_FAILS_IN_WINDOW)));
    }

    @Override
    public long ioHealthHangMillis() {
        return Long.parseLong(config.get(KEY_IO_HEALTH_HANG_MILL,
                String.valueOf(DEFAULT_IO_HEALTH_HANG_MILL)));
    }

    @Override
    public long ioHealthMinWindowRecords() {
        return Long.parseLong(config.get(KEY_IO_HEALTH_MIN_WINDOW_RECORDS,
                String.valueOf(DEFAULT_IO_HEALTH_MIN_WINDOW_RECORDS)));
    }

    @Override
    public long ioHealthSlowMillis() {
        return Long.parseLong(config.get(KEY_IO_HEALTH_SLOW_MILL,
                String.valueOf(DEFAULT_IO_HEALTH_SLOW_MILL)));
    }

    @Override
    public long ioHealthWindowSeconds() {
        return Long.parseLong(config.get(KEY_IO_HEALTH_WINDOW_SEC,
                String.valueOf(DEFAULT_IO_HEALTH_WINDOW_SEC)));
    }

    @Override
    public long ioHealthFileWindowSeconds() {
        return Long.parseLong(config.get(KEY_IO_HEALTH_FILE_WINDOW_SEC,
                String.valueOf(DEFAULT_IO_HEALTH_FILE_WINDOW_SEC)));
    }

    @Override
    public long ioHealthStateIdleSeconds() {
        return Long.parseLong(config.get(KEY_IO_HEALTH_STATE_IDLE_SEC,
                String.valueOf(DEFAULT_IO_HEALTH_STATE_IDLE_SEC)));
    }

    @Override
    public String ioHealthDisabledRules() {
        return config.get(KEY_IO_HEALTH_DISABLED_RULES, "");
    }

    @Override
    public long ioHealthProbeIntervalMill() {
        return Long.parseLong(config.get(KEY_IO_HEALTH_PROBE_INTERVAL_MILL,
                String.valueOf(DEFAULT_IO_HEALTH_PROBE_INTERVAL_MILL)));
    }

    @Override
    public int ioHealthProbeSlowMinRecords() {
        return Integer.parseInt(config.get(KEY_IO_HEALTH_PROBE_SLOW_MIN_RECORDS,
                String.valueOf(DEFAULT_IO_HEALTH_PROBE_SLOW_MIN_RECORDS)));
    }

    @Override
    public int ioHealthProbeSlowRatioPercent() {
        return Integer.parseInt(config.get(KEY_IO_HEALTH_PROBE_SLOW_RATIO_PERCENT,
                String.valueOf(DEFAULT_IO_HEALTH_PROBE_SLOW_RATIO_PERCENT)));
    }

    @Override
    public long ioHealthProbeWindowSeconds() {
        return Long.parseLong(config.get(KEY_IO_HEALTH_PROBE_WINDOW_SEC,
                String.valueOf(DEFAULT_IO_HEALTH_PROBE_WINDOW_SEC)));
    }

    private String getDefaultRdsDir() {
        return System.getProperty("user.dir");
    }
}
