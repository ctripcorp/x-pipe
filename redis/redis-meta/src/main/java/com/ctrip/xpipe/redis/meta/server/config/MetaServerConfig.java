package com.ctrip.xpipe.redis.meta.server.config;

import com.ctrip.xpipe.redis.core.config.CoreConfig;
import com.ctrip.xpipe.redis.core.meta.DcInfo;
import com.ctrip.xpipe.redis.meta.server.keeper.elect.KeeperElectStrategy;

import java.util.Map;
import java.util.Set;

/**
 * @author marsqing
 *
 *         Jun 16, 2016 11:48:44 AM
 */
public interface MetaServerConfig extends CoreConfig {

	String getConsoleAddress();

	int getMetaRefreshMilli();

	int getMetaServerId();

	String getMetaServerIp();

	int getMetaServerPort();

	int getClusterServersRefreshMilli();

	int getSlotRefreshMilli();

	int getLeaderCheckMilli();

	Map<String, DcInfo> getDcInofs();

	int getWaitforOffsetMilli();

	boolean validateDomain();

	int getKeeperSetIndexInterval();

	int getKeeperInfoCheckInterval();

	int getApplierInfoCheckInterval();

	int getWaitForMetaSyncDelayMilli();

	Set<String> getOwnClusterType();

	boolean shouldCorrectPeerMasterPeriodically();

	long getNewMasterCacheTimeoutMilli();

	String getChooseRouteStrategyType();

	int getConsoleNotifycationTaskQueueSize();

	KeeperElectStrategy getKeeperElectStrategy();

	/**
	 * TFS Gateway Host（含 scheme，无 path）。QConfig: {@code metaserver.tfs.gateway.host}。
	 */
	String getTfsGatewayHost();

	/**
	 * TFS Gateway appId，拼入 ForceCloseDir URL path。默认 0。
	 */
	long getTfsGatewayAppId();

	String getTfsDirPathTemplate();

	default int getKeeperContainerAddressMaxRemovedPercent() {
		return 50;
	}

	default int getKeeperContainerAddressMinRemovedHostnames() {
		return 5;
	}

	/** Orphan-store GC round: off by default (opt-in, destructive feature). */
	default boolean isOrphanGcEnabled() {
		return false;
	}

	/**
	 * Orphan-store GC round: when true (the default), compute and log candidates as usual but never
	 * actually call the container's delete endpoint. Flip to false only after a dry-run period has
	 * been reviewed.
	 */
	default boolean isOrphanGcDryRun() {
		return true;
	}

	/**
	 * How long a {@code DcMetaCache} load from console may go un-refreshed before the orphan-store GC
	 * round refuses to trust it for this round (keeper-tfs-m7 §2.2①). Default 2 minutes.
	 */
	default long getOrphanGcMetaFreshMilli() {
		return 120_000L;
	}

	/** Orphan-store GC round period. Not time-sensitive — default 1 hour. */
	default long getOrphanGcRoundIntervalMilli() {
		return 3_600_000L;
	}

	/**
	 * Orphan-store GC round: how many {@code tfsFsId} groups may be processed concurrently. Groups are
	 * independent (different containers, different occupied sets), so this is pure fan-out — kept
	 * modest by default (4) since it's a background task that should never compete meaningfully with
	 * normal traffic for container-side resources.
	 */
	default int getOrphanGcGroupParallelism() {
		return 4;
	}

	/**
	 * Orphan-store GC round: replIds per gc request. Leadership, meta freshness and occupancy are
	 * re-checked before every chunk, so this bounds how much one stale leader can still send; also
	 * keeps one request well under the 5s container client timeout. Default 10.
	 */
	default int getOrphanGcBatchSize() {
		return 10;
	}

}
