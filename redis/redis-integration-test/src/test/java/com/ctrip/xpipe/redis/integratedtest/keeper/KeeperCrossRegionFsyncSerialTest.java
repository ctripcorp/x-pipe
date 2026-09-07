package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.Redis;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.SLAVE_STATE;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.impl.CrossRegionFsyncCoordinator;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static com.ctrip.xpipe.redis.core.protocal.MASTER_STATE.REDIS_REPL_CONNECTED;

/**
 * 验证 cross-region keeper 在「keeper 切换 + 增量不可继续」场景下，
 * 对多个 cross-region slave 的串行全量同步：同一时刻最多 maxLoadingSlaves 个 slave 在 loading，
 * 且按 ip:port 字典序依次放行。
 */
public class KeeperCrossRegionFsyncSerialTest extends AbstractKeeperIntegratedSingleDc {

    private static final int CROSS_REGION_SLAVE_COUNT = 3;

    private static final int GRACE_SECONDS = 5;

    protected String getXpipeMetaConfigFile() {
        return "integrated-keeper-fullseq-test.xml";
    }

    @Override
    protected KeeperConfig getKeeperConfig() {
        TestKeeperConfig config = new TestKeeperConfig();
        config.setReplicationStoreMaxCommandsToTransferBeforeCreateRdb(Integer.MAX_VALUE);
        config.setReplicationStoreGcIntervalSeconds(1000000);
        config.setReplicationStoreCommandFileSize(1024);
        // 串行全量：一次只允许 1 个 cross-region slave 全量
        config.setMaxLoadingSlaves(1);
        config.setCrossRegionFsyncGraceSeconds(GRACE_SECONDS);
        config.setCrossRegionFsyncSettleSeconds(2);
        return config;
    }

    @Test
    public void testSwitchThenCrossRegionSlavesSerialFsyncByIpOrder() throws Exception {
        KeeperMeta firstActive = getKeeperActive();
        KeeperMeta backup = getKeepersBackup().iterator().next();
        waitKeeperConnected(firstActive, backup);

        // 单 DC 拓扑下，3 个 redis slave 都挂在 active keeper 下，即 3 个 cross-region slave
        List<RedisMeta> crossRegionSlaves = getRedisSlaves();
        Assert.assertEquals(CROSS_REGION_SLAVE_COUNT, crossRegionSlaves.size());
        waitAllSlavesOnline(crossRegionSlaves);

        // 先写入一批数据，让 3 个 slave 走增量（online 且数据一致）
        sendMessageToMaster(redisMaster, 20);
        sleep(2000);

        // 模拟 keeper 切换
        switchActiveKeeper(firstActive, backup);
        waitKeeperConnected(backup, firstActive);

        // 新 active keeper 变为 cross-region（切换触发的 reconnect 会重置 crossRegion，故在切换完成后再置位）
        DefaultRedisKeeperServer newActiveServer = (DefaultRedisKeeperServer) getRedisKeeperServer(backup);
        newActiveServer.setCrossRegion(true);
        for (int round = 0; round < 2; round++) {
            // 模拟「增量出问题」：SLAVEOF NO ONE 使 replid 变化 + 写分歧数据，再统一指回新 active keeper
            for (RedisMeta slave : crossRegionSlaves) {
                jedisExecCommand(slave.getIp(), slave.getPort(), "SLAVEOF", "NO", "ONE");
                jedisExecCommand(slave.getIp(), slave.getPort(), "SET", "diverge_" + slave.getPort(), "1");
            }
            for (RedisMeta slave : crossRegionSlaves) {
                setRedisMaster(slave, new HostPort(backup.getIp(), backup.getPort()));
            }

        // 采样验证：全量串行（同一时刻最多 1 个 loading）且按 ip:port 升序
        List<RedisMeta> fullSyncOrder = sampleFullSyncOrder(newActiveServer, crossRegionSlaves);
        Assert.assertEquals("all cross-region slaves should do full sync", CROSS_REGION_SLAVE_COUNT, fullSyncOrder.size());
        assertAscendingByIpPort(fullSyncOrder);

        // 等 grace 过期，避免下一轮重连时命中旧租约「续跑」绕过串行
        sleep(GRACE_SECONDS * 1000 + 1000);
        }

        // 全量完成后数据与 master 一致（分歧数据被 RDB 覆盖清除）
        assertRedisEquals(redisMaster, crossRegionSlaves);
    }

    /**
     * 验证「全量同步进行中异步切换 keeper」：切换打断旧 active 上正在进行的全量，
     * slave 重连到新 active 后重新按 ip:port 串行放行，且任意时刻最多 1 个 loading。
     */
    @Test
    public void testSwitchKeeperDuringCrossRegionSerialFsync() throws Exception {
        KeeperMeta firstActive = getKeeperActive();
        KeeperMeta backup = getKeepersBackup().iterator().next();
        waitKeeperConnected(firstActive, backup);

        List<RedisMeta> crossRegionSlaves = getRedisSlaves();
        Assert.assertEquals(CROSS_REGION_SLAVE_COUNT, crossRegionSlaves.size());
        waitAllSlavesOnline(crossRegionSlaves);

        // 先写入一批数据
        sendMessageToMaster(redisMaster, 20);
        sleep(2000);

        // 旧 active keeper 置为 cross-region
        DefaultRedisKeeperServer firstActiveServer = (DefaultRedisKeeperServer) getRedisKeeperServer(firstActive);
        firstActiveServer.setCrossRegion(true);

        // 模拟「增量出问题」：SLAVEOF NO ONE 使 replid 变化 + 写分歧数据，再统一指回旧 active
        for (RedisMeta slave : crossRegionSlaves) {
            jedisExecCommand(slave.getIp(), slave.getPort(), "SLAVEOF", "NO", "ONE");
            jedisExecCommand(slave.getIp(), slave.getPort(), "SET", "diverge_" + slave.getPort(), "1");
        }
        for (RedisMeta slave : crossRegionSlaves) {
            setRedisMaster(slave, new HostPort(firstActive.getIp(), firstActive.getPort()));
        }

        // 等全量真正开始（至少一个 slave 进入 loading）
        waitAtLeastOneLoading(firstActiveServer);

        // 全量进行中，异步切换 keeper
        final Throwable[] switchError = new Throwable[1];
        Thread switchThread = new Thread(() -> {
            try {
                switchActiveKeeper(firstActive, backup);
            } catch (Throwable th) {
                switchError[0] = th;
            }
        }, "switch-keeper-during-fsync");
        switchThread.start();
        switchThread.join();
        Assert.assertNull("switch keeper failed", switchError[0]);
        waitKeeperConnected(backup, firstActive);

        // 新 active 置为 cross-region（必须在 slave 重连之前，否则绕过串行）
        DefaultRedisKeeperServer newActiveServer = (DefaultRedisKeeperServer) getRedisKeeperServer(backup);
        newActiveServer.setCrossRegion(true);

        // 模拟「增量出问题」：SLAVEOF NO ONE 使 replid 变化 + 写分歧数据，再统一指回旧 active
        for (RedisMeta slave : crossRegionSlaves) {
            jedisExecCommand(slave.getIp(), slave.getPort(), "SLAVEOF", "NO", "ONE");
            jedisExecCommand(slave.getIp(), slave.getPort(), "SET", "diverge_" + slave.getPort(), "1");
        }
        for (RedisMeta slave : crossRegionSlaves) {
            setRedisMaster(slave, new HostPort(backup.getIp(), backup.getPort()));
        }

        // 采样验证：全量串行（同一时刻最多 1 个 loading）且按 ip:port 升序
        List<RedisMeta> fullSyncOrder = sampleFullSyncOrder(newActiveServer, crossRegionSlaves);
        Assert.assertEquals("all cross-region slaves should do full sync", CROSS_REGION_SLAVE_COUNT, fullSyncOrder.size());
        assertAscendingByIpPort(fullSyncOrder);

        // 全量完成后数据与 master 一致（分歧数据被 RDB 覆盖清除）
        assertRedisEquals(redisMaster, crossRegionSlaves);
    }

    /**
     * 验证「拉入」：不切换 keeper，直接让 cross-region keeper 的多个 slave 全量同步，
     * 同一时刻最多 maxLoadingSlaves 个 loading，且按 ip:port 字典序依次放行。
     */
    @Test
    public void testCrossRegionSlavesPullInSerialFsyncByIpOrder() throws Exception {
        KeeperMeta active = getKeeperActive();
        List<RedisMeta> crossRegionSlaves = getRedisSlaves();
        Assert.assertEquals(CROSS_REGION_SLAVE_COUNT, crossRegionSlaves.size());
        waitAllSlavesOnline(crossRegionSlaves);

        // 先写入一批数据，让 slave 走增量并数据一致
        sendMessageToMaster(redisMaster, 2000);
        sleep(2000);

        // 直接标记 active keeper 为 cross-region（不切换 keeper，直接测「拉入」）
        DefaultRedisKeeperServer activeServer = (DefaultRedisKeeperServer) getRedisKeeperServer(active);
        activeServer.setCrossRegion(true);

        for (int round = 0; round < 2; round++) {
            // 模拟「增量不可继续」：SLAVEOF NO ONE 使 replid 变化 + 写分歧数据，再统一指回 active keeper
            for (RedisMeta slave : crossRegionSlaves) {
                jedisExecCommand(slave.getIp(), slave.getPort(), "SLAVEOF", "NO", "ONE");
                jedisExecCommand(slave.getIp(), slave.getPort(), "SET", "diverge_" + slave.getPort(), "1");
            }
            for (RedisMeta slave : crossRegionSlaves) {
                setRedisMaster(slave, new HostPort(active.getIp(), active.getPort()));
            }

            // 采样验证：全量串行（同一时刻最多 1 个 loading）且按 ip:port 升序
            List<RedisMeta> fullSyncOrder = sampleFullSyncOrder(activeServer, crossRegionSlaves);
            Assert.assertEquals("all cross-region slaves should do full sync", CROSS_REGION_SLAVE_COUNT, fullSyncOrder.size());
            assertAscendingByIpPort(fullSyncOrder);

            // 等 grace 过期，避免下一轮重连时命中旧租约「续跑」绕过串行
            sleep(GRACE_SECONDS * 1000 + 1000);
        }

        // 全量完成后数据与 master 一致（分歧数据被 RDB 覆盖清除）
        assertRedisEquals(redisMaster, crossRegionSlaves);
    }

    /**
     * 验证「增量加载持续失败」：打开 breakDownstreamCommands 开关后，下游 slave 反复全量/增量失败，
     * 但任意时刻最多 1 个 loading（不并发）；关闭开关后 slave 恢复 online 且数据一致。
     */
    @Test
    public void testIncrementalBreakFallbackToFullSync() throws Exception {
        KeeperMeta active = getKeeperActive();
        List<RedisMeta> crossRegionSlaves = getRedisSlaves();
        Assert.assertEquals(CROSS_REGION_SLAVE_COUNT, crossRegionSlaves.size());
        waitAllSlavesOnline(crossRegionSlaves);

        sendMessageToMaster(redisMaster, 20);
        sleep(2000);

        DefaultRedisKeeperServer activeServer = (DefaultRedisKeeperServer) getRedisKeeperServer(active);
        activeServer.setCrossRegion(true);

        TestKeeperConfig config = (TestKeeperConfig) activeServer.getKeeperConfig();
        // 调小 maxTransfer：任何待传增量命令都会命中「too much to transfer」→ 回落全量
        config.setReplicationStoreMaxCommandsToTransferBeforeCreateRdb(1);
        // 打开开关：增量加载持续失败
        config.setBreakDownstreamCommands(true);

        // 模拟「增量出问题」：SLAVEOF NO ONE 使 replid 变化 + 写分歧数据，再统一指回 keeper
        for (RedisMeta slave : crossRegionSlaves) {
            jedisExecCommand(slave.getIp(), slave.getPort(), "SLAVEOF", "NO", "ONE");
            jedisExecCommand(slave.getIp(), slave.getPort(), "SET", "diverge_" + slave.getPort(), "1");
        }
        for (RedisMeta slave : crossRegionSlaves) {
            setRedisMaster(slave, new HostPort(active.getIp(), active.getPort()));
        }

        // 开关打开期间：增量失败 + maxTransfer 阈值回落全量，slave 反复全量，但任意时刻最多 1 个 loading
        assertNoConcurrentLoadingDuring(activeServer, 15_000);

        // 关闭开关 + 恢复 maxTransfer：slave 恢复 online 且数据一致
        config.setBreakDownstreamCommands(false);
        config.setReplicationStoreMaxCommandsToTransferBeforeCreateRdb(Integer.MAX_VALUE);
        waitAllSlavesOnline(crossRegionSlaves);
        assertRedisEquals(redisMaster, crossRegionSlaves);
    }

    /**
     * 周期采样 keeper 的 slave 状态，返回 slave 进入 loading 状态的先后顺序。
     * 期间断言：任意时刻处于 loading 的 cross-region slave 数不超过 1（串行）。
     */
    private List<RedisMeta> sampleFullSyncOrder(DefaultRedisKeeperServer server, List<RedisMeta> slaves) {
        Map<String, RedisMeta> byKey = new HashMap<>();
        for (RedisMeta s : slaves) {
            byKey.put(s.getIp() + ":" + s.getPort(), s);
        }

        CrossRegionFsyncCoordinator coordinator = server.getCrossRegionFsyncCoordinator();
        int baseline = coordinator.admitOrder4Test().size();
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            int loading = 0;
            Set<RedisSlave> fseqSlaves = server.slaves();
            for (RedisSlave rs : fseqSlaves) {
                if (rs.isKeeper()) continue;
                SLAVE_STATE st = rs.getSlaveState();
                if (st == SLAVE_STATE.REDIS_REPL_WAIT_RDB_DUMPING || st == SLAVE_STATE.REDIS_REPL_SEND_BULK) {
                    loading++;
                }
            }
            Assert.assertTrue("concurrent cross-region full sync detected: " + loading, loading <= 1);

            List<String> admitOrder = coordinator.admitOrder4Test();
            List<String> newAdmits = new ArrayList<>(admitOrder.subList(baseline, admitOrder.size()));
            if (newAdmits.size() >= slaves.size() && loading == 0 && allOnline(server.slaves())) {
                List<RedisMeta> order = new ArrayList<>();
                for (String key : newAdmits) {
                    RedisMeta meta = byKey.get(key);
                    Assert.assertNotNull("unknown admitted key: " + key, meta);
                    order.add(meta);
                }
                return order;
            }
            sleep(30);
        }

        Assert.fail("full sync did not complete in time, admitOrder: " + coordinator.admitOrder4Test());
        return null;
    }

    private boolean allOnline(Set<RedisSlave> slaves) {
        for (RedisSlave rs : slaves) {
            if (rs.isKeeper()) continue;
            if (rs.getSlaveState() != SLAVE_STATE.REDIS_REPL_ONLINE) return false;
        }
        return true;
    }

    private void assertAscendingByIpPort(List<RedisMeta> actual) {
        List<RedisMeta> expected = new ArrayList<>(actual);
        expected.sort(Comparator.comparing(RedisMeta::getIp).thenComparingInt(RedisMeta::getPort));
        Assert.assertEquals("full sync should be serialized in ascending ip:port order", expected, actual);
    }

    private void waitKeeperConnected(KeeperMeta active, KeeperMeta backup) throws Exception {
        waitConditionUntilTimeOut(() ->
                getRedisKeeperServer(active).getRedisMaster().getMasterState().equals(REDIS_REPL_CONNECTED));
        waitConditionUntilTimeOut(() ->
                getRedisKeeperServer(backup).getRedisMaster().getMasterState().equals(REDIS_REPL_CONNECTED));
    }

    private void switchActiveKeeper(KeeperMeta oldActive, KeeperMeta newActive) throws Exception {
        oldActive.setActive(false);
        newActive.setActive(true);
        makeKeeperRight();
        sleep(2000);
    }

    private void waitAllSlavesOnline(List<RedisMeta> slaves) throws Exception {
        for (RedisMeta slave : slaves) {
            waitSlaveOnline(slave.getIp(), slave.getPort());
        }
    }

    private void waitAtLeastOneLoading(DefaultRedisKeeperServer server) throws Exception {
        waitConditionUntilTimeOut(() -> {
            for (RedisSlave rs : server.slaves()) {
                if (rs.isKeeper()) continue;
                SLAVE_STATE st = rs.getSlaveState();
                if (st == SLAVE_STATE.REDIS_REPL_WAIT_RDB_DUMPING || st == SLAVE_STATE.REDIS_REPL_SEND_BULK) {
                    return true;
                }
            }
            return false;
        });
    }

    private void assertNoConcurrentLoadingDuring(DefaultRedisKeeperServer server, long durationMillis) {
        long deadline = System.currentTimeMillis() + durationMillis;
        while (System.currentTimeMillis() < deadline) {
            int loading = 0;
            for (RedisSlave rs : server.slaves()) {
                if (rs.isKeeper()) continue;
                SLAVE_STATE st = rs.getSlaveState();
                if (st == SLAVE_STATE.REDIS_REPL_WAIT_RDB_DUMPING || st == SLAVE_STATE.REDIS_REPL_SEND_BULK) {
                    loading++;
                }
            }
            Assert.assertTrue("concurrent cross-region full sync detected: " + loading, loading <= 1);
            sleep(30);
        }
    }
}
