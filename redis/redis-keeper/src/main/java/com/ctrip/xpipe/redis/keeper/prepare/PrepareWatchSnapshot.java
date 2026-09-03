package com.ctrip.xpipe.redis.keeper.prepare;

/**
 * Watcher 周期刷新的观测快照（D11 / D30）。INFO / ROLE 在 Redis 命令线程上只读本对象，禁止触发 FS。
 * {@code getCommandsLastUpdatedAt()} 恒 0，不进快照。
 */
public final class PrepareWatchSnapshot {

	private final long totalLength;

	private final long backlogEndOffset;

	public PrepareWatchSnapshot(long totalLength, long backlogEndOffset) {
		this.totalLength = totalLength;
		this.backlogEndOffset = backlogEndOffset;
	}

	public long getTotalLength() {
		return totalLength;
	}

	public long getBacklogEndOffset() {
		return backlogEndOffset;
	}
}
