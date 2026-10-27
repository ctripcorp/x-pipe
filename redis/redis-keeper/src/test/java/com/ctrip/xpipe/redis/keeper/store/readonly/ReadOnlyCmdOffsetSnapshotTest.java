package com.ctrip.xpipe.redis.keeper.store.readonly;

import org.junit.Assert;
import org.junit.Test;

/**
 * Phase HC (T-HC.5 ①): {@link ReadOnlyCmdOffsetSnapshot}. D48 ④ / §4.2.3b.
 */
public class ReadOnlyCmdOffsetSnapshotTest {

	@Test
	public void testStartOffsetOfFloorsToOwningSegment() {
		ReadOnlyCmdOffsetSnapshot snapshot =
				new ReadOnlyCmdOffsetSnapshot(320L, new long[]{100L, 200L, 300L}, 7L);

		Assert.assertEquals(100L, snapshot.startOffsetOf(100L));
		Assert.assertEquals(100L, snapshot.startOffsetOf(199L));
		Assert.assertEquals(200L, snapshot.startOffsetOf(200L));
		Assert.assertEquals(200L, snapshot.startOffsetOf(299L));
		Assert.assertEquals(300L, snapshot.startOffsetOf(300L));
		Assert.assertEquals(300L, snapshot.startOffsetOf(320L));
		// 快照可能比 FS 实际状态旧：超出观察末尾仍按 floor 返回末段起点，越界由 Reader 的区间校验判
		Assert.assertEquals(300L, snapshot.startOffsetOf(999L));

		Assert.assertEquals(320L, snapshot.getObservedEnd());
		Assert.assertEquals(100L, snapshot.getFirstOffset());
		Assert.assertEquals(3, snapshot.getSegmentCount());
		Assert.assertEquals(7L, snapshot.getObservedAtMillis());
	}

	@Test
	public void testStartOffsetOfBeforeFirstSegment() {
		ReadOnlyCmdOffsetSnapshot snapshot = new ReadOnlyCmdOffsetSnapshot(250L, new long[]{100L, 200L}, 1L);

		Assert.assertEquals(ReadOnlyCmdOffsetSnapshot.OFFSET_BEFORE_FIRST_SEGMENT, snapshot.startOffsetOf(99L));
		Assert.assertEquals(ReadOnlyCmdOffsetSnapshot.OFFSET_BEFORE_FIRST_SEGMENT, snapshot.startOffsetOf(0L));
	}

	@Test
	public void testEmptyDirSnapshot() {
		Assert.assertEquals(0L, ReadOnlyCmdOffsetSnapshot.EMPTY.getObservedEnd());
		Assert.assertEquals(0L, ReadOnlyCmdOffsetSnapshot.EMPTY.getFirstOffset());
		Assert.assertEquals(0, ReadOnlyCmdOffsetSnapshot.EMPTY.getSegmentCount());
		Assert.assertEquals(ReadOnlyCmdOffsetSnapshot.OFFSET_BEFORE_FIRST_SEGMENT,
				ReadOnlyCmdOffsetSnapshot.EMPTY.startOffsetOf(0L));

		ReadOnlyCmdOffsetSnapshot empty = new ReadOnlyCmdOffsetSnapshot(0L, null, 3L);
		Assert.assertEquals(0L, empty.getFirstOffset());
		Assert.assertEquals(ReadOnlyCmdOffsetSnapshot.OFFSET_BEFORE_FIRST_SEGMENT, empty.startOffsetOf(10L));
	}

	@Test
	public void testImmutableSegmentStartsAndWithObservedEnd() {
		long[] starts = new long[]{300L, 100L, 200L};
		ReadOnlyCmdOffsetSnapshot snapshot = new ReadOnlyCmdOffsetSnapshot(350L, starts, 2L);

		// 构造时升序排列并与入参数组解耦
		starts[0] = 999L;
		Assert.assertArrayEquals(new long[]{100L, 200L, 300L}, snapshot.getSegmentStarts());
		snapshot.getSegmentStarts()[0] = 999L;
		Assert.assertEquals(100L, snapshot.getFirstOffset());

		ReadOnlyCmdOffsetSnapshot advanced = snapshot.withObservedEnd(400L);
		Assert.assertEquals(400L, advanced.getObservedEnd());
		Assert.assertEquals(350L, snapshot.getObservedEnd());
		Assert.assertArrayEquals(snapshot.getSegmentStarts(), advanced.getSegmentStarts());
	}
}
