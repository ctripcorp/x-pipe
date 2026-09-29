package com.ctrip.xpipe.redis.keeper.store.readonly;

import java.util.Arrays;

/**
 * 只读 cmd 的 offset 内存快照（m5 D48 ④ / §4.2.3b）。
 * <p>
 * 关相期间三个句柄都不在，{@code totalLength()} / {@code lowestAvailableOffset()} /
 * {@code startOffsetOf()} 必须有内存来源，一律读本快照，从此零 FS 调用。
 * <p>
 * <b>同步策略</b>：不可变对象 + {@code volatile} 引用整份替换就是全部同步策略。写者只有
 * Watcher 一条线程，不需要 CAS；读者每次只读一次引用，保证看到的字段来自同一个快照。
 * 没有 generation / lineage / dirty flag 之类的旁路变量（D49）。
 */
public final class ReadOnlyCmdOffsetSnapshot {

	/**
	 * 空目录（未观察到任何段）。{@code observedEnd == 0} / {@code firstOffset == 0}。
	 */
	public static final ReadOnlyCmdOffsetSnapshot EMPTY = new ReadOnlyCmdOffsetSnapshot(0L, new long[0], 0L);

	/**
	 * 与 {@code AsyncFileSystem.getStartOffsetByReadOffset} 同语义：读点落在首段左侧。
	 */
	public static final long OFFSET_BEFORE_FIRST_SEGMENT = -1L;

	private final long observedEnd;

	/**
	 * 升序段起点。
	 */
	private final long[] segmentStarts;

	private final long firstOffset;

	private final long observedAtMillis;

	public ReadOnlyCmdOffsetSnapshot(long observedEnd, long[] segmentStarts, long observedAtMillis) {
		long[] starts = segmentStarts == null ? new long[0] : segmentStarts.clone();
		Arrays.sort(starts);
		this.segmentStarts = starts;
		this.firstOffset = starts.length == 0 ? 0L : starts[0];
		this.observedEnd = Math.max(observedEnd, 0L);
		this.observedAtMillis = observedAtMillis;
	}

	public long getObservedEnd() {
		return observedEnd;
	}

	public long getFirstOffset() {
		return firstOffset;
	}

	public long getObservedAtMillis() {
		return observedAtMillis;
	}

	public int getSegmentCount() {
		return segmentStarts.length;
	}

	/**
	 * 升序段起点副本（不暴露内部数组）。
	 */
	public long[] getSegmentStarts() {
		return segmentStarts.clone();
	}

	/**
	 * 段起点 floor 二分：返回包含 {@code readOffset} 的段起点；落在首段左侧或空目录返回
	 * {@link #OFFSET_BEFORE_FIRST_SEGMENT}。纯内存，不碰 FS。
	 */
	public long startOffsetOf(long readOffset) {
		if (segmentStarts.length == 0 || readOffset < segmentStarts[0]) {
			return OFFSET_BEFORE_FIRST_SEGMENT;
		}
		int idx = Arrays.binarySearch(segmentStarts, readOffset);
		if (idx >= 0) {
			return segmentStarts[idx];
		}
		// binarySearch 未命中时返回 -(insertion point) - 1，floor 即 insertion point 的前一个
		int insertion = -idx - 1;
		return segmentStarts[insertion - 1];
	}

	/**
	 * 段链条不变、只改观察末尾（{@code refreshTotalLength} 的防御性取 max 用）。
	 */
	ReadOnlyCmdOffsetSnapshot withObservedEnd(long newObservedEnd) {
		return new ReadOnlyCmdOffsetSnapshot(newObservedEnd, segmentStarts, observedAtMillis);
	}

	@Override
	public String toString() {
		return String.format("ReadOnlyCmdOffsetSnapshot{observedEnd=%d, firstOffset=%d, segments=%d, observedAt=%d}",
				observedEnd, firstOffset, segmentStarts.length, observedAtMillis);
	}
}
