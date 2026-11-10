package com.ctrip.xpipe.redis.keeper.config;

/**
 * @author wenchao.meng
 *
 * Aug 22, 2016
 */
public class TestKeeperContainerConfig implements KeeperContainerConfig{

	private String replicationStoreDir;
	private String mode = MODE_NORMAL;
	private String metaServerUrl = System.getProperty("metaServerUrl", "http://localhost:9747");

	private long ioHealthCheckIntervalMill = DEFAULT_IO_HEALTH_CHECK_INTERVAL_MILL;
	private long ioHealthWarmupMill = DEFAULT_IO_HEALTH_WARMUP_MILL;
	private int ioHealthMinRecords = DEFAULT_IO_HEALTH_MIN_RECORDS;
	private long ioHealthMetricIntervalMill = DEFAULT_IO_HEALTH_METRIC_INTERVAL_MILL;
	private int ioHealthFailRatioPercent = DEFAULT_IO_HEALTH_FAIL_RATIO_PERCENT;
	private int ioHealthSlowRatioPercent = DEFAULT_IO_HEALTH_SLOW_RATIO_PERCENT;
	private int ioHealthTenantAbnormalPercent = DEFAULT_IO_HEALTH_TENANT_ABNORMAL_PERCENT;
	private int ioHealthTenantAbnormalFloor = DEFAULT_IO_HEALTH_TENANT_ABNORMAL_FLOOR;
	private int ioHealthFileConsecutiveFails = DEFAULT_IO_HEALTH_FILE_CONSECUTIVE_FAILS;
	private int ioHealthFileFailsInWindow = DEFAULT_IO_HEALTH_FILE_FAILS_IN_WINDOW;
	private long ioHealthHangMillis = DEFAULT_IO_HEALTH_HANG_MILL;
	private long ioHealthMinWindowRecords = DEFAULT_IO_HEALTH_MIN_WINDOW_RECORDS;
	private long ioHealthSlowMillis = DEFAULT_IO_HEALTH_SLOW_MILL;
	private long ioHealthWindowSeconds = DEFAULT_IO_HEALTH_WINDOW_SEC;
	private long ioHealthFileWindowSeconds = DEFAULT_IO_HEALTH_FILE_WINDOW_SEC;
	private long ioHealthStateIdleSeconds = DEFAULT_IO_HEALTH_STATE_IDLE_SEC;
	private String ioHealthDisabledRules = "";
	
	public TestKeeperContainerConfig(String replicationStoreDir) {
		this.replicationStoreDir = replicationStoreDir;
	}

	@Override
	public String getReplicationStoreDir() {
		return replicationStoreDir;
	}

	@Override
	public String getMode() {
		return mode;
	}

	@Override
	public boolean isTfsMode() {
		return MODE_TFS.equalsIgnoreCase(mode);
	}

	@Override
	public int diskCheckInterval() {
		return 30000;
	}

	@Override
	public int checkRoundBeforeMarkDown() {
		return 3;
	}

	@Override
	public int keeperLeaderResetMinInterval() {
		return 10;
	}

	public void setMetaServerUrl(String metaServerUrl) {
		this.metaServerUrl = metaServerUrl;
	}
	
	public void setReplicationStoreDir(String replicationStoreDir) {
		this.replicationStoreDir = replicationStoreDir;
	}

	public void setMode(String mode) {
		this.mode = mode;
	}

	@Override
	public long ioHealthCheckIntervalMill() {
		return ioHealthCheckIntervalMill;
	}

	public TestKeeperContainerConfig setIoHealthCheckIntervalMill(long ioHealthCheckIntervalMill) {
		this.ioHealthCheckIntervalMill = ioHealthCheckIntervalMill;
		return this;
	}

	@Override
	public long ioHealthWarmupMill() {
		return ioHealthWarmupMill;
	}

	public TestKeeperContainerConfig setIoHealthWarmupMill(long ioHealthWarmupMill) {
		this.ioHealthWarmupMill = ioHealthWarmupMill;
		return this;
	}

	@Override
	public int ioHealthMinRecords() {
		return ioHealthMinRecords;
	}

	public TestKeeperContainerConfig setIoHealthMinRecords(int ioHealthMinRecords) {
		this.ioHealthMinRecords = ioHealthMinRecords;
		return this;
	}

	@Override
	public long ioHealthMetricIntervalMill() {
		return ioHealthMetricIntervalMill;
	}

	public TestKeeperContainerConfig setIoHealthMetricIntervalMill(long ioHealthMetricIntervalMill) {
		this.ioHealthMetricIntervalMill = ioHealthMetricIntervalMill;
		return this;
	}

	@Override
	public int ioHealthFailRatioPercent() {
		return ioHealthFailRatioPercent;
	}

	public TestKeeperContainerConfig setIoHealthFailRatioPercent(int ioHealthFailRatioPercent) {
		this.ioHealthFailRatioPercent = ioHealthFailRatioPercent;
		return this;
	}

	@Override
	public int ioHealthSlowRatioPercent() {
		return ioHealthSlowRatioPercent;
	}

	public TestKeeperContainerConfig setIoHealthSlowRatioPercent(int ioHealthSlowRatioPercent) {
		this.ioHealthSlowRatioPercent = ioHealthSlowRatioPercent;
		return this;
	}

	@Override
	public int ioHealthTenantAbnormalPercent() {
		return ioHealthTenantAbnormalPercent;
	}

	public TestKeeperContainerConfig setIoHealthTenantAbnormalPercent(int ioHealthTenantAbnormalPercent) {
		this.ioHealthTenantAbnormalPercent = ioHealthTenantAbnormalPercent;
		return this;
	}

	@Override
	public int ioHealthTenantAbnormalFloor() {
		return ioHealthTenantAbnormalFloor;
	}

	public TestKeeperContainerConfig setIoHealthTenantAbnormalFloor(int ioHealthTenantAbnormalFloor) {
		this.ioHealthTenantAbnormalFloor = ioHealthTenantAbnormalFloor;
		return this;
	}

	@Override
	public int ioHealthFileConsecutiveFails() {
		return ioHealthFileConsecutiveFails;
	}

	public TestKeeperContainerConfig setIoHealthFileConsecutiveFails(int ioHealthFileConsecutiveFails) {
		this.ioHealthFileConsecutiveFails = ioHealthFileConsecutiveFails;
		return this;
	}

	@Override
	public int ioHealthFileFailsInWindow() {
		return ioHealthFileFailsInWindow;
	}

	public TestKeeperContainerConfig setIoHealthFileFailsInWindow(int ioHealthFileFailsInWindow) {
		this.ioHealthFileFailsInWindow = ioHealthFileFailsInWindow;
		return this;
	}

	@Override
	public long ioHealthHangMillis() {
		return ioHealthHangMillis;
	}

	public TestKeeperContainerConfig setIoHealthHangMillis(long ioHealthHangMillis) {
		this.ioHealthHangMillis = ioHealthHangMillis;
		return this;
	}

	@Override
	public long ioHealthMinWindowRecords() {
		return ioHealthMinWindowRecords;
	}

	public TestKeeperContainerConfig setIoHealthMinWindowRecords(long ioHealthMinWindowRecords) {
		this.ioHealthMinWindowRecords = ioHealthMinWindowRecords;
		return this;
	}

	@Override
	public String ioHealthDisabledRules() {
		return ioHealthDisabledRules;
	}

	@Override
	public long ioHealthProbeIntervalMill() {
		return DEFAULT_IO_HEALTH_PROBE_INTERVAL_MILL;
	}

	@Override
	public int ioHealthProbeSlowMinRecords() {
		return DEFAULT_IO_HEALTH_PROBE_SLOW_MIN_RECORDS;
	}

	@Override
	public int ioHealthProbeSlowRatioPercent() {
		return DEFAULT_IO_HEALTH_PROBE_SLOW_RATIO_PERCENT;
	}

	@Override
	public long ioHealthProbeWindowSeconds() {
		return DEFAULT_IO_HEALTH_PROBE_WINDOW_SEC;
	}

	public TestKeeperContainerConfig setIoHealthDisabledRules(String ioHealthDisabledRules) {
		this.ioHealthDisabledRules = ioHealthDisabledRules;
		return this;
	}

	@Override
	public long ioHealthSlowMillis() {
		return ioHealthSlowMillis;
	}

	public TestKeeperContainerConfig setIoHealthSlowMillis(long ioHealthSlowMillis) {
		this.ioHealthSlowMillis = ioHealthSlowMillis;
		return this;
	}

	@Override
	public long ioHealthWindowSeconds() {
		return ioHealthWindowSeconds;
	}

	public TestKeeperContainerConfig setIoHealthWindowSeconds(long ioHealthWindowSeconds) {
		this.ioHealthWindowSeconds = ioHealthWindowSeconds;
		return this;
	}

	@Override
	public long ioHealthFileWindowSeconds() {
		return ioHealthFileWindowSeconds;
	}

	public TestKeeperContainerConfig setIoHealthFileWindowSeconds(long ioHealthFileWindowSeconds) {
		this.ioHealthFileWindowSeconds = ioHealthFileWindowSeconds;
		return this;
	}

	@Override
	public long ioHealthStateIdleSeconds() {
		return ioHealthStateIdleSeconds;
	}

	public TestKeeperContainerConfig setIoHealthStateIdleSeconds(long ioHealthStateIdleSeconds) {
		this.ioHealthStateIdleSeconds = ioHealthStateIdleSeconds;
		return this;
	}
}
