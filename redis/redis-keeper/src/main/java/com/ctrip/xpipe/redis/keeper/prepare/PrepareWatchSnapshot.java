package com.ctrip.xpipe.redis.keeper.prepare;

import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;

/**
 * Watcher 周期刷新的观测快照（D11 / D30）。INFO / ROLE 在 Redis 命令线程上只读本对象，禁止触发 FS。
 * Store 相关字段在此；slave 相关（{@code connected_slaves}）问 {@code RedisKeeperServer}。
 * {@code getCommandsLastUpdatedAt()} 恒 0，不进快照。
 */
public final class PrepareWatchSnapshot {

	private final long totalLength;

	private final long backlogEndOffset;

	/**
	 * Same formula as {@code ReplicationStore.getCurReplStageReplOff()} (last inclusive repl offset).
	 * {@code 0} when {@code curReplStage} was null at refresh.
	 */
	private final long replOffset;

	private final String masterReplId;

	private final String masterReplId2;

	private final long secondReplOffset;

	private final long backlogFirstByteOffset;

	public PrepareWatchSnapshot(long totalLength, long backlogEndOffset, long replOffset) {
		this(totalLength, backlogEndOffset, replOffset,
				ReplicationStoreMeta.EMPTY_REPL_ID, ReplicationStoreMeta.EMPTY_REPL_ID,
				ReplicationStoreMeta.DEFAULT_SECOND_REPLID_OFFSET, 0L);
	}

	public PrepareWatchSnapshot(long totalLength, long backlogEndOffset, long replOffset,
								String masterReplId, String masterReplId2,
								long secondReplOffset, long backlogFirstByteOffset) {
		this.totalLength = totalLength;
		this.backlogEndOffset = backlogEndOffset;
		this.replOffset = replOffset;
		this.masterReplId = masterReplId == null ? ReplicationStoreMeta.EMPTY_REPL_ID : masterReplId;
		this.masterReplId2 = masterReplId2 == null ? ReplicationStoreMeta.EMPTY_REPL_ID : masterReplId2;
		this.secondReplOffset = secondReplOffset;
		this.backlogFirstByteOffset = backlogFirstByteOffset;
	}

	public long getTotalLength() {
		return totalLength;
	}

	public long getBacklogEndOffset() {
		return backlogEndOffset;
	}

	public long getReplOffset() {
		return replOffset;
	}

	public String getMasterReplId() {
		return masterReplId;
	}

	public String getMasterReplId2() {
		return masterReplId2;
	}

	public long getSecondReplOffset() {
		return secondReplOffset;
	}

	public long getBacklogFirstByteOffset() {
		return backlogFirstByteOffset;
	}
}
