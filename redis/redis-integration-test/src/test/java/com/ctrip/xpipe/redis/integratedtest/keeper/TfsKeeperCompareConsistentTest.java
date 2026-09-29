package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.redis.comparator.compare.CompareLane;
import com.ctrip.xpipe.redis.comparator.stream.StreamRingBuffer;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.TimeoutException;

/**
 * TFS Keeper 与进程内 Comparator 一致路径集成串测（spec §4.12.4 / AC-23～AC-26）。
 */
public class TfsKeeperCompareConsistentTest extends AbstractTfsKeeperIntegrated {

	private static final int INCREMENT_KEYS = 20;

	private static final int INCREMENT_VALUE_BYTES = 512;

	@Test
	public void testNormalIncrementMonotonicallyCompared() throws Exception {
		startComparator();
		waitAligned();
		long before = comparatorHarness.comparedBytes();

		long firstBatchBytes = writeRandomBatch(getRedisMaster(), "tfs-hn-normal-1-");
		waitComparedBytesGrowBy(before, firstBatchBytes);
		long afterFirstBatch = comparatorHarness.comparedBytes();

		long secondBatchBytes = writeRandomBatch(getRedisMaster(), "tfs-hn-normal-2-");
		waitComparedBytesGrowBy(afterFirstBatch, secondBatchBytes);
		long afterSecondBatch = comparatorHarness.comparedBytes();

		Assert.assertTrue(afterFirstBatch >= before + firstBatchBytes);
		Assert.assertTrue(afterSecondBatch >= afterFirstBatch + secondBatchBytes);
		Assert.assertEquals(0L, comparatorHarness.mismatchCount());
	}

	@Test
	public void testKeeperRoleSwitchKeepsComparing() throws Exception {
		startComparator();
		waitAligned();
		KeeperMeta oldActive = activeKeeper;
		KeeperMeta oldPrepare = backupKeeper;
		RedisMeta master = getRedisMaster();
		StreamRingBuffer[] oldBuffers = laneBuffers();
		int[] beforeReconnect = laneReconnectCounts();
		long beforeMismatch = comparatorHarness.mismatchCount();

		// 先释放 TFS 写槽，再由旧 Prepare 占槽。
		setKeeperState(oldActive, KeeperState.PREPARE, master.getIp(), master.getPort());
		waitTfsPrepareReady(oldActive);
		setKeeperState(oldPrepare, KeeperState.ACTIVE, master.getIp(), master.getPort());
		waitTfsActiveReady(oldPrepare);
		waitLaneGenerationsChanged(oldBuffers, beforeReconnect);
		waitAligned();
		long beforeCompared = comparatorHarness.comparedBytes();

		long batchBytes = writeRandomBatch(master, "tfs-hn-keeper-switch-");
		waitComparedBytesGrowBy(beforeCompared, batchBytes);

		Assert.assertTrue(comparatorHarness.comparedBytes() >= beforeCompared + batchBytes);
		Assert.assertEquals(beforeMismatch, comparatorHarness.mismatchCount());
	}

	@Test
	public void testRedisReplIdSwitchReconnectsWithoutMismatch() throws Exception {
		startComparator();
		waitAligned();
		RedisMeta oldMaster = getRedisMaster();
		RedisMeta promoted = new RedisMeta().setIp("127.0.0.1").setPort(randomPort());
		startRedis(promoted, oldMaster);
		waitSlaveOnline(promoted.getIp(), promoted.getPort());
		waitSlavesReplOffsetCatchUp(oldMaster, Collections.singletonList(promoted));
		StreamRingBuffer[] oldBuffers = laneBuffers();
		int[] beforeReconnect = laneReconnectCounts();
		long beforeReplIdMismatch = comparatorHarness.comparator().getReplIdMismatchCount();
		long beforeMismatch = comparatorHarness.mismatchCount();

		try (Jedis jedis = createJedis(promoted)) {
			jedis.slaveofNoOne();
		}
		setKeeperState(activeKeeper, KeeperState.ACTIVE, promoted.getIp(), promoted.getPort());
		waitTfsActiveReady(activeKeeper);
		waitReplIdMismatchGrow(beforeReplIdMismatch);
		waitLaneGenerationsChanged(oldBuffers, beforeReconnect);
		waitAligned();
		long beforeCompared = comparatorHarness.comparedBytes();

		long batchBytes = writeRandomBatch(promoted, "tfs-hn-replid-switch-");
		waitComparedBytesGrowBy(beforeCompared, batchBytes);

		Assert.assertTrue(comparatorHarness.comparator().getReplIdMismatchCount() > beforeReplIdMismatch);
		Assert.assertEquals(beforeMismatch, comparatorHarness.mismatchCount());
		Assert.assertTrue(comparatorHarness.comparedBytes() >= beforeCompared + batchBytes);
	}

	@Test
	public void testFullResyncSwitchesStoreAndRecovers() throws Exception {
		startComparator();
		waitAligned();
		KeeperMeta occupyingKeeper = activeKeeper;
		KeeperMeta prepareKeeper = backupKeeper;
		RedisMeta master = getRedisMaster();
		File oldStoreDir = openedStoreDir(prepareKeeper);
		Assert.assertNotNull(oldStoreDir);
		ReplicationStore occupyingStore = tfsStoreManager(occupyingKeeper).getOpenedStore();
		Assert.assertTrue(occupyingStore instanceof DefaultReplicationStore);
		Assert.assertEquals(oldStoreDir, ((DefaultReplicationStore) occupyingStore).getBaseDir());
		StreamRingBuffer[] oldBuffers = laneBuffers();
		int[] beforeReconnect = laneReconnectCounts();
		long beforeMismatch = comparatorHarness.mismatchCount();
		int beforeSyncFull = redisSyncFull(master);
		RedisKeeperServer prepareServer = getRedisKeeperServer(prepareKeeper);
		Assert.assertFalse("Comparator slave should be attached before store switch",
				prepareServer.slaves().isEmpty());
		RedisKeeperServer oldServer = getRedisKeeperServer(occupyingKeeper);
		oldServer.stop();
		oldServer.dispose();
		occupyingStore.destroy();
		remove(oldServer);

		startKeeper(occupyingKeeper);
		setKeeperState(occupyingKeeper, KeeperState.ACTIVE, master.getIp(), master.getPort());
		waitStoreDirChanged(occupyingKeeper, oldStoreDir);
		waitPrepareStoreReleased(prepareKeeper);

		waitSyncFullGrow(master, beforeSyncFull);
		waitTfsActiveReady(occupyingKeeper);
		waitLaneGenerationsChanged(oldBuffers, beforeReconnect);
		waitAligned();
		long beforeCompared = comparatorHarness.comparedBytes();

		long batchBytes = writeRandomBatch(master, "tfs-hn-fullresync-");
		waitComparedBytesGrowBy(beforeCompared, batchBytes);

		Assert.assertNotEquals(oldStoreDir, openedStoreDir(occupyingKeeper));
		Assert.assertTrue(redisSyncFull(master) > beforeSyncFull);
		Assert.assertEquals(beforeMismatch, comparatorHarness.mismatchCount());
		Assert.assertTrue(comparatorHarness.comparedBytes() >= beforeCompared + batchBytes);
	}

	/**
	 * 返回 key + value 的最小复制字节数；真实 RESP 还包含命令名与 framing。
	 * 以此作为下界可排除心跳或切换期间少量在途字节造成的假增长。
	 */
	private long writeRandomBatch(RedisMeta redis, String keyPrefix) {
		long minimumComparedBytes = 0;
		try (Jedis jedis = createJedis(redis)) {
			for (int i = 0; i < INCREMENT_KEYS; i++) {
				String key = keyPrefix + i;
				String value = randomString(INCREMENT_VALUE_BYTES);
				jedis.set(key, value);
				minimumComparedBytes += key.getBytes(StandardCharsets.UTF_8).length;
				minimumComparedBytes += value.getBytes(StandardCharsets.UTF_8).length;
			}
		}
		return minimumComparedBytes;
	}

	private void waitComparedBytesGrowBy(long from, long minimumBytes) throws Exception {
		try {
			waitConditionUntilTimeOut(() -> comparatorHarness != null
						&& comparatorHarness.comparedBytes() >= from + minimumBytes,
					READY_WAIT_MILLI, READY_POLL_MILLI);
		} catch (TimeoutException e) {
			TimeoutException dump = new TimeoutException(describeComparatorWait(
					"waitComparedBytesGrowBy from=" + from + " minimumBytes=" + minimumBytes));
			dump.initCause(e);
			throw dump;
		}
	}

	private void waitReplIdMismatchGrow(long from) throws Exception {
		try {
			waitConditionUntilTimeOut(() -> comparatorHarness != null
						&& comparatorHarness.comparator().getReplIdMismatchCount() > from,
					READY_WAIT_MILLI, READY_POLL_MILLI);
		} catch (TimeoutException e) {
			TimeoutException dump = new TimeoutException(
					describeComparatorWait("waitReplIdMismatchGrow from=" + from));
			dump.initCause(e);
			throw dump;
		}
	}

	private void waitStoreDirChanged(KeeperMeta keeper, File oldStoreDir) throws Exception {
		try {
			waitConditionUntilTimeOut(() -> {
				File current = openedStoreDir(keeper);
				return current != null && !oldStoreDir.equals(current);
			}, READY_WAIT_MILLI, READY_POLL_MILLI);
		} catch (TimeoutException e) {
			TimeoutException dump = new TimeoutException(describeFullResyncWait(
					"waitStoreDirChanged old=" + oldStoreDir, keeper));
			dump.initCause(e);
			throw dump;
		}
	}

	private void waitPrepareStoreReleased(KeeperMeta prepare) throws Exception {
		RedisKeeperServer server = getRedisKeeperServer(prepare);
		try {
			waitConditionUntilTimeOut(() -> server.getOpenedStore() == null && server.slaves().isEmpty(),
					READY_WAIT_MILLI, READY_POLL_MILLI);
		} catch (TimeoutException e) {
			TimeoutException dump = new TimeoutException(
					describeFullResyncWait("waitPrepareStoreReleased", prepare));
			dump.initCause(e);
			throw dump;
		}
	}

	private void waitSyncFullGrow(RedisMeta redis, int from) throws Exception {
		try {
			waitConditionUntilTimeOut(() -> {
				try {
					return redisSyncFull(redis) > from;
				} catch (Exception e) {
					logger.warn("[waitSyncFullGrow] redis={} from={}", redis, from, e);
					return false;
				}
			}, READY_WAIT_MILLI, READY_POLL_MILLI);
		} catch (TimeoutException e) {
			TimeoutException dump = new TimeoutException(describeComparatorWait(
					"waitSyncFullGrow redis=" + redis + " from=" + from));
			dump.initCause(e);
			throw dump;
		}
	}

	private void waitLaneGenerationsChanged(StreamRingBuffer[] oldBuffers, int[] oldReconnectCounts)
			throws Exception {
		try {
			waitConditionUntilTimeOut(() -> laneGenerationsChanged(oldBuffers, oldReconnectCounts),
					READY_WAIT_MILLI, READY_POLL_MILLI);
		} catch (TimeoutException e) {
			TimeoutException dump = new TimeoutException(
					describeComparatorWait("waitLaneGenerationsChanged"));
			dump.initCause(e);
			throw dump;
		}
	}

	private boolean laneGenerationsChanged(StreamRingBuffer[] oldBuffers, int[] oldReconnectCounts) {
		CompareLane[] lanes = comparatorHarness.comparator().getLanes();
		if (lanes.length != oldBuffers.length || lanes.length != oldReconnectCounts.length) {
			return false;
		}
		for (int i = 0; i < lanes.length; i++) {
			if (lanes[i].getStreamReconnectCount() <= oldReconnectCounts[i]
					|| lanes[i].getReplId() == null
					|| lanes[i].getBuffer() == oldBuffers[i]) {
				return false;
			}
		}
		return true;
	}

	private StreamRingBuffer[] laneBuffers() {
		CompareLane[] lanes = comparatorHarness.comparator().getLanes();
		StreamRingBuffer[] buffers = new StreamRingBuffer[lanes.length];
		for (int i = 0; i < lanes.length; i++) {
			buffers[i] = lanes[i].getBuffer();
		}
		return buffers;
	}

	private int[] laneReconnectCounts() {
		CompareLane[] lanes = comparatorHarness.comparator().getLanes();
		int[] reconnectCounts = new int[lanes.length];
		for (int i = 0; i < lanes.length; i++) {
			reconnectCounts[i] = lanes[i].getStreamReconnectCount();
		}
		return reconnectCounts;
	}

	private int totalReconnectCount() {
		int total = 0;
		if (comparatorHarness == null) {
			return total;
		}
		for (CompareLane lane : comparatorHarness.comparator().getLanes()) {
			total += lane.getStreamReconnectCount();
		}
		return total;
	}

	private int redisSyncFull(RedisMeta redis) throws Exception {
		return Integer.parseInt(infoRedis(redis.getIp(), redis.getPort(),
				InfoCommand.INFO_TYPE.STATS, "sync_full"));
	}

	private File openedStoreDir(KeeperMeta keeper) {
		return openedStoreDir(tfsStoreManager(keeper).getOpenedStore());
	}

	private File openedStoreDir(ReplicationStore opened) {
		return opened instanceof DefaultReplicationStore
				? ((DefaultReplicationStore) opened).getBaseDir() : null;
	}

	private String describeFullResyncWait(String prefix, KeeperMeta keeper) {
		return describeComparatorWait(prefix)
				+ " | keeper=" + tfsLogKey(keeper)
				+ " openedStoreDir=" + openedStoreDir(keeper)
				+ " reconnectCount=" + totalReconnectCount();
	}

}
