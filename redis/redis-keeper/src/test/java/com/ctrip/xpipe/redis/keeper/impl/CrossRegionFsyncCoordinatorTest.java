package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.redis.keeper.RedisSlave;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.ctrip.xpipe.redis.keeper.SLAVE_STATE.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class CrossRegionFsyncCoordinatorTest {

    private static final long SETTLE_MILLIS = 2000;

    private CrossRegionFsyncCoordinator coordinator;
    private AtomicLong clock;
    private List<RedisSlave> released;

    @Before
    public void setup() {
        clock = new AtomicLong(0);
        coordinator = new CrossRegionFsyncCoordinator(() -> 1, () -> 5000L, () -> SETTLE_MILLIS, clock::get);
        released = new ArrayList<>();
    }

    private RedisSlave slave(String ip, int port) {
        RedisSlave s = mock(RedisSlave.class);
        when(s.ip()).thenReturn(ip);
        when(s.getSlaveListeningPort()).thenReturn(port);
        when(s.isKeeper()).thenReturn(false);
        when(s.isOpen()).thenReturn(true);
        when(s.getSlaveState()).thenReturn(REDIS_REPL_WAIT_SEQ_FSYNC);   // 默认：等待放行
        when(s.getAck()).thenReturn(null);
        when(s.isColdStart()).thenReturn(false);
        return s;
    }

    /** 模拟调度器放行 → 异步 doFullSync 进入 loading */
    private void admit(RedisSlave s) {
        released.add(s);
        when(s.getSlaveState()).thenReturn(REDIS_REPL_WAIT_RDB_DUMPING);
    }

    private void loading(RedisSlave s) {
        when(s.getSlaveState()).thenReturn(REDIS_REPL_WAIT_RDB_DUMPING);
    }

    private void online(RedisSlave s) {
        when(s.getSlaveState()).thenReturn(REDIS_REPL_ONLINE);
        when(s.getAck()).thenReturn(100L);
        when(s.getGraceStart()).thenReturn(clock.get());   // 全量完成（ack putOnline）时刻
    }

    private void disconnect(RedisSlave s) {
        when(s.isOpen()).thenReturn(false);
    }

    private Set<RedisSlave> slaveSet(RedisSlave... slaves) {
        return new HashSet<>(Arrays.asList(slaves));
    }

    @Test
    public void testNewSlaveGoesWaiting() {
        RedisSlave a = slave("10.0.0.2", 6379);
        assertFalse(coordinator.onFullSyncRequest(a));
        assertEquals(0, coordinator.occupiedCount4Test(slaveSet(a)));
        assertEquals(1, coordinator.waitingCount4Test(slaveSet(a)));
    }

    @Test
    public void testColdStartAdmitDirectly() {
        RedisSlave a = slave("10.0.0.2", 6379);
        when(a.isColdStart()).thenReturn(true);
        assertTrue(coordinator.onFullSyncRequest(a));
    }

    @Test
    public void testLoadingReentrantReturnTrue() {
        RedisSlave a = slave("10.0.0.2", 6379);
        loading(a);                                          // 已在全量中（dumper 续跑）
        assertTrue(coordinator.onFullSyncRequest(a));
    }

    @Test
    public void testReentrantWhileWaitingReturnsFalse() {
        RedisSlave a = slave("10.0.0.2", 6379);
        assertFalse(coordinator.onFullSyncRequest(a));       // WAITING
        assertFalse(coordinator.onFullSyncRequest(a));       // 同对象重入仍不越权放行
        assertEquals(1, coordinator.waitingCount4Test(slaveSet(a)));
    }

    @Test
    public void testClosedSlaveNotAdmitted() {
        RedisSlave a = slave("10.0.0.2", 6379);
        disconnect(a);
        assertFalse(coordinator.onFullSyncRequest(a));
    }

    @Test
    public void testDynamicMaxLoadingSlavesCnt() {
        AtomicInteger max = new AtomicInteger(-1);
        CrossRegionFsyncCoordinator c = new CrossRegionFsyncCoordinator(max::get, () -> 5000L, () -> SETTLE_MILLIS, clock::get);
        RedisSlave a = slave("10.0.0.2", 6379);

        assertTrue(c.onFullSyncRequest(a));   // max=-1（禁用）→ 直接放行

        max.set(1);                           // 动态改 max=1，无需重建协调器
        assertFalse(c.onFullSyncRequest(a));  // 新请求 → defer
    }

    @Test
    public void testTickReleaseInIpOrder() {
        RedisSlave a = slave("10.0.0.2", 6379);
        RedisSlave b = slave("10.0.0.3", 6379);
        RedisSlave c = slave("10.0.0.4", 6379);
        coordinator.tick(slaveSet(a, b, c), this::admit);   // 起结算窗口
        clock.set(SETTLE_MILLIS);
        coordinator.tick(slaveSet(a, b, c), this::admit);   // 满窗口，放行 a（IP 最小）
        assertEquals(1, released.size());
        assertSame(a, released.get(0));
    }

    @Test
    public void testMultipleSlotsAdmitConcurrently() {
        CrossRegionFsyncCoordinator multi = new CrossRegionFsyncCoordinator(() -> 2, () -> 5000L, () -> SETTLE_MILLIS, clock::get);
        RedisSlave a = slave("10.0.0.2", 6379);
        RedisSlave b = slave("10.0.0.3", 6379);
        RedisSlave c = slave("10.0.0.4", 6379);
        multi.tick(slaveSet(a, b, c), this::admit);   // 起结算窗口
        clock.set(SETTLE_MILLIS);
        multi.tick(slaveSet(a, b, c), this::admit);   // 满窗口，放行 a、b
        assertEquals(2, released.size());
        assertSame(a, released.get(0));
        assertSame(b, released.get(1));
    }

    @Test
    public void testFullSyncDoneEnterGrace() {
        RedisSlave a = slave("10.0.0.2", 6379);
        online(a);
        coordinator.tick(slaveSet(a), this::admit);
        assertEquals(1, coordinator.occupiedCount4Test(slaveSet(a)));   // grace 占名额
    }

    @Test
    public void testGraceReleaseAfterExpire() {
        RedisSlave a = slave("10.0.0.2", 6379);
        online(a);
        coordinator.tick(slaveSet(a), this::admit);   // grace 起点 0
        assertEquals(1, coordinator.occupiedCount4Test(slaveSet(a)));

        clock.set(5000);
        coordinator.tick(slaveSet(a), this::admit);   // 满 5s
        assertEquals(0, coordinator.occupiedCount4Test(slaveSet(a)));
    }

    @Test
    public void testGraceSameSlaveReenterDefer() {
        RedisSlave a = slave("10.0.0.2", 6379);
        online(a);
        coordinator.tick(slaveSet(a), this::admit);   // 进 grace

        assertFalse(coordinator.onFullSyncRequest(a));   // 相同 slave，grace 内 defer，不重复全量
    }

    @Test
    public void testSyncFailReleaseImmediately() {
        RedisSlave a = slave("10.0.0.2", 6379);
        loading(a);                                          // 全量中
        assertEquals(1, coordinator.occupiedCount4Test(slaveSet(a)));

        disconnect(a);                                       // 全量中断链
        assertEquals(0, coordinator.occupiedCount4Test(slaveSet(a)));   // 立即不再占名额
    }

    @Test
    public void testDisconnectedWaitingSlaveRemoved() {
        RedisSlave a = slave("10.0.0.2", 6379);
        disconnect(a);                                       // 等待中断链
        coordinator.tick(slaveSet(a), this::admit);
        assertEquals(0, released.size());                    // 不放行已断开的等待 slave
    }

    @Test
    public void testSettleWindow() {
        RedisSlave a = slave("10.0.0.2", 6379);
        coordinator.tick(slaveSet(a), this::admit);   // 起结算窗口，不放行
        assertEquals(0, released.size());

        clock.set(1000);                               // 1s < 2s，仍在结算中
        coordinator.tick(slaveSet(a), this::admit);
        assertEquals(0, released.size());

        clock.set(SETTLE_MILLIS);                      // 满 2s，放行
        coordinator.tick(slaveSet(a), this::admit);
        assertEquals(1, released.size());
        assertSame(a, released.get(0));
    }
}
