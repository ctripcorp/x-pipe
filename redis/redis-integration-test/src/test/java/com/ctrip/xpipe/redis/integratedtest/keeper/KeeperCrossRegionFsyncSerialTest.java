package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.Redis;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.SLAVE_STATE;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
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
        config.setCrossRegionFsyncGraceSeconds(5);
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
        }

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
        }

        // 全量完成后数据与 master 一致（分歧数据被 RDB 覆盖清除）
        assertRedisEquals(redisMaster, crossRegionSlaves);
    }

    /**
     * 周期采样 keeper 的 slave 状态，返回 slave 进入 loading 状态的先后顺序。
     * 期间断言：任意时刻处于 loading 的 cross-region slave 数不超过 1（串行）。
     */
    private List<RedisMeta> sampleFullSyncOrder(DefaultRedisKeeperServer server, List<RedisMeta> slaves) {
        Map<Integer, RedisMeta> byPort = new HashMap<>();
        for (RedisMeta s : slaves) {
            byPort.put(s.getPort(), s);
        }

        LinkedHashSet<Integer> admittedOrder = new LinkedHashSet<>();
        Set<Integer> seenWaiting = new HashSet<>();
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            int loading = 0;
            Set<RedisSlave> fseqSlaves = server.slaves();
            for (RedisSlave rs : fseqSlaves) {
                if (rs.isKeeper()) continue;
                int port = rs.getSlaveListeningPort();
                SLAVE_STATE st = rs.getSlaveState();
                if(st != SLAVE_STATE.REDIS_REPL_ONLINE) {
                    logger.info("[fullsyncslave] slave {}, state {}", rs, st);
                }
                if (st == SLAVE_STATE.REDIS_REPL_WAIT_RDB_DUMPING || st == SLAVE_STATE.REDIS_REPL_SEND_BULK) {
                    loading++;
                }
                if(st == SLAVE_STATE.REDIS_REPL_WAIT_SEQ_FSYNC){
                    seenWaiting.add(port);
                }else if(seenWaiting.contains(port)) {
                    admittedOrder.add(port);
                }
            }
            Assert.assertTrue("concurrent cross-region full sync detected: " + loading, loading <= 1);

            if (admittedOrder.size() >= slaves.size()) {
                break;
            }
            sleep(30);
        }

        List<RedisMeta> order = new ArrayList<>();
        for (int port : admittedOrder) {
            RedisMeta meta = byPort.get(port);
            Assert.assertNotNull("unknown slave listening port: " + port, meta);
            order.add(meta);
        }
        return order;
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

}
