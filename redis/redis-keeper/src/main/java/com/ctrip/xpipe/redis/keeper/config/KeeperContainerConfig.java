package com.ctrip.xpipe.redis.keeper.config;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public interface KeeperContainerConfig {

    String MODE_NORMAL = "NORMAL";

    String MODE_TFS = "TFS";

    // Container TFS IO health detection (TfsHealthChecker), read once when the container starts.
    String KEY_IO_HEALTH_CHECK_INTERVAL_MILL = "io.health.check.interval.mill";
    long DEFAULT_IO_HEALTH_CHECK_INTERVAL_MILL = 5000L;
    String KEY_IO_HEALTH_WARMUP_MILL = "io.health.warmup.mill";
    long DEFAULT_IO_HEALTH_WARMUP_MILL = 30_000L;
    String KEY_IO_HEALTH_MIN_RECORDS = "io.health.min.records";
    int DEFAULT_IO_HEALTH_MIN_RECORDS = 100;
    String KEY_IO_HEALTH_METRIC_INTERVAL_MILL = "io.health.metric.interval.mill";
    long DEFAULT_IO_HEALTH_METRIC_INTERVAL_MILL = 60_000L;
    String KEY_IO_HEALTH_FAIL_RATIO_PERCENT = "io.health.fail.ratio.percent";
    int DEFAULT_IO_HEALTH_FAIL_RATIO_PERCENT = 1;
    String KEY_IO_HEALTH_SLOW_RATIO_PERCENT = "io.health.slow.ratio.percent";
    int DEFAULT_IO_HEALTH_SLOW_RATIO_PERCENT = 5;
    String KEY_IO_HEALTH_TENANT_ABNORMAL_PERCENT = "io.health.tenant.abnormal.percent";
    int DEFAULT_IO_HEALTH_TENANT_ABNORMAL_PERCENT = 5;
    String KEY_IO_HEALTH_TENANT_ABNORMAL_FLOOR = "io.health.tenant.abnormal.floor";
    int DEFAULT_IO_HEALTH_TENANT_ABNORMAL_FLOOR = 5;
    String KEY_IO_HEALTH_FILE_CONSECUTIVE_FAILS = "io.health.file.consecutive.fails";
    int DEFAULT_IO_HEALTH_FILE_CONSECUTIVE_FAILS = 3;
    String KEY_IO_HEALTH_FILE_FAILS_IN_WINDOW = "io.health.file.fails.in.window";
    int DEFAULT_IO_HEALTH_FILE_FAILS_IN_WINDOW = 5;
    String KEY_IO_HEALTH_HANG_MILL = "io.health.hang.mill";
    long DEFAULT_IO_HEALTH_HANG_MILL = 5_000L;
    String KEY_IO_HEALTH_MIN_WINDOW_RECORDS = "io.health.min.window.records";
    long DEFAULT_IO_HEALTH_MIN_WINDOW_RECORDS = 1_000L;
    String KEY_IO_HEALTH_SLOW_MILL = "io.health.slow.mill";
    long DEFAULT_IO_HEALTH_SLOW_MILL = 2_000L;
    String KEY_IO_HEALTH_WINDOW_SEC = "io.health.window.sec";
    long DEFAULT_IO_HEALTH_WINDOW_SEC = 30L;
    String KEY_IO_HEALTH_FILE_WINDOW_SEC = "io.health.file.window.sec";
    long DEFAULT_IO_HEALTH_FILE_WINDOW_SEC = 10L;
    String KEY_IO_HEALTH_STATE_IDLE_SEC = "io.health.state.idle.sec";
    long DEFAULT_IO_HEALTH_STATE_IDLE_SEC = 60L;
    /** Comma separated rule names to switch off, e.g. {@code fileFailure,slowRatio}. */
    String KEY_IO_HEALTH_DISABLED_RULES = "io.health.disabled.rules";
    String KEY_IO_HEALTH_PROBE_INTERVAL_MILL = "io.health.probe.interval.mill";
    long DEFAULT_IO_HEALTH_PROBE_INTERVAL_MILL = 5000L;
    String KEY_IO_HEALTH_PROBE_SLOW_MIN_RECORDS = "io.health.probe.slow.min.records";
    int DEFAULT_IO_HEALTH_PROBE_SLOW_MIN_RECORDS = 3;
    String KEY_IO_HEALTH_PROBE_SLOW_RATIO_PERCENT = "io.health.probe.slow.ratio.percent";
    int DEFAULT_IO_HEALTH_PROBE_SLOW_RATIO_PERCENT = 67;
    String KEY_IO_HEALTH_PROBE_WINDOW_SEC = "io.health.probe.window.sec";
    long DEFAULT_IO_HEALTH_PROBE_WINDOW_SEC = 15L;
    String KEY_IO_HEALTH_RECOVERED_PENDING_AUTO_RECOVER = "io.health.recovered.pending.auto.recover";
    boolean DEFAULT_IO_HEALTH_RECOVERED_PENDING_AUTO_RECOVER = false;

    String getReplicationStoreDir();

    /**
     * Process-level store path mode: {@link #MODE_NORMAL} or {@link #MODE_TFS}.
     */
    String getMode();

    boolean isTfsMode();

    int diskCheckInterval();

    int checkRoundBeforeMarkDown();

    int keeperLeaderResetMinInterval();

    long ioHealthCheckIntervalMill();

    long ioHealthWarmupMill();

    int ioHealthMinRecords();

    long ioHealthMetricIntervalMill();

    int ioHealthFailRatioPercent();

    int ioHealthSlowRatioPercent();

    int ioHealthTenantAbnormalPercent();

    int ioHealthTenantAbnormalFloor();

    int ioHealthFileConsecutiveFails();

    int ioHealthFileFailsInWindow();

    /** Task execution time above this is reported as a hang (default 5s). */
    long ioHealthHangMillis();

    /** Finished tasks the window must hold before the ratio rules judge it (default 1000). */
    long ioHealthMinWindowRecords();

    /** Execution time above this is counted as slow (default 2s). */
    long ioHealthSlowMillis();

    /** Rule window length in seconds; clamped to what the telemetry ring can serve (default 30). */
    long ioHealthWindowSeconds();

    /** Per-file failure window in seconds (default 10). */
    long ioHealthFileWindowSeconds();

    /** A tenant/file with no IO for this long is dropped from the telemetry (default 60). */
    long ioHealthStateIdleSeconds();

    /** Comma separated rule names switched off for this container; empty means all enabled. */
    String ioHealthDisabledRules();

    long ioHealthProbeIntervalMill();

    int ioHealthProbeSlowMinRecords();

    int ioHealthProbeSlowRatioPercent();

    long ioHealthProbeWindowSeconds();

    boolean ioHealthRecoveredPendingAutoRecover();

}
