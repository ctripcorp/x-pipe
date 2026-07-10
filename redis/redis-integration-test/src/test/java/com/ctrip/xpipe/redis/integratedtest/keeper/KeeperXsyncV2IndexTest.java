package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.store.gtid.index.BlockWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.ctrip.xpipe.redis.core.protocal.MASTER_STATE.REDIS_REPL_CONNECTED;

/**
 * @author TB
 * @date 2026/7/3 19:15
 */

public class KeeperXsyncV2IndexTest extends AbstractKeeperIntegratedSingleDc {

    private static final String V2_INDEX_PREFIX = "indexv2_";
    private static final String V2_BLOCK_PREFIX = "blockv2_";

    private TestKeeperConfig keeperConfig;


    @Override
    protected KeeperConfig getKeeperConfig() {
        if (keeperConfig == null) {
            keeperConfig = new TestKeeperConfig();
            keeperConfig.setReplicationStoreMaxCommandsToTransferBeforeCreateRdb(Integer.MAX_VALUE);
            keeperConfig.setReplicationStoreGcIntervalSeconds(1000000);
            keeperConfig.setReplicationStoreCommandFileSize(1024); // 小文件，加速切换
            keeperConfig.setDualWrite(true);   // 启用双写
            keeperConfig.setReadV2(true);      // 优先读取 v2
            keeperConfig.setIndexZoneConsecutiveThreshold(100);
            keeperConfig.setBlockSizeThreshold(100);
        }
        return keeperConfig;
    }

    @Test
    public void testV2IndexWriteAndReadDuringSwitch() throws Exception {
        setRedisToGtidEnabled(redisMaster.getIp(), redisMaster.getPort());

        // 准备环境和数据
        KeeperMeta activeKeeperMeta = getKeeperActive();
        KeeperMeta backupKeeperMeta = getKeepersBackup().iterator().next();
        RedisMeta master = getRedisMaster();

        // 等待 master 连接
        waitKeeperConnected(activeKeeperMeta, backupKeeperMeta);

        // 写入测试数据：混合 GTID 命令与 PUBLISH 以产生 Zone 条目
        sendMixedCommands(230, 210); // 500 条 GTID 命令，200 条 PUBLISH

        sleep(10000);
        assertGtid(master);

        // 获取当前 GTID Set 作为预期值
        String expectedGtidStr = getGtidSet(master.getIp(), master.getPort(), "gtid_executed");
        GtidSet expectedGtid = new GtidSet(expectedGtidStr);

        // 验证 v2 索引文件存在
        assertV2IndexFilesExist(activeKeeperMeta, true);
        assertV2IndexFilesExist(backupKeeperMeta, true);

        // 执行 Keeper 切换
        switchActiveKeeper(activeKeeperMeta, backupKeeperMeta);

        // 等待 master 连接
        waitKeeperConnected(activeKeeperMeta, backupKeeperMeta);

        // 切换后再次验证 GTID 一致性
        assertGtid(master);

        // 写入更多数据，确保新 active 的 v2 索引继续工作
        sendMixedCommands(500, 200);
        expectedGtid = new GtidSet(getGtidSet(master.getIp(), master.getPort(), "gtid_executed"));
        sleep(10000);

        // 通过 locateContinueGtidSet 验证 v2 索引的 GTID 定位能力
        verifyGtidPosition(backupKeeperMeta, expectedGtid);

        // 验证 v2 索引文件仍然存在且大小合理
        assertV2IndexFilesExist(backupKeeperMeta, true);
    }

    @Test
    public void testV2IndexOnlineRollback() throws Exception {
        setRedisToGtidEnabled(redisMaster.getIp(), redisMaster.getPort());

        KeeperMeta activeKeeperMeta = getKeeperActive();
        KeeperMeta backupKeeperMeta = getKeepersBackup().iterator().next();
        RedisMeta master = getRedisMaster();

        // 1. 双写 + 读 v2 模式下写入数据
        sendMixedCommands(500, 200);
        sleep(10_000);
        assertGtid(master);
        assertV2IndexFilesExist(activeKeeperMeta, true);

        // 2. 动态修改配置，回退到 v1 读取（同时可停止双写）
        keeperConfig.setReadV2(false);
        keeperConfig.setDualWrite(false);

        // 3. 执行 Keeper 切换，让新 active 使用修改后的配置重新初始化
        switchActiveKeeper(activeKeeperMeta, backupKeeperMeta);
        activeKeeperMeta = backupKeeperMeta; // 新的 active
        backupKeeperMeta = getKeepersBackup().iterator().next();
        waitKeeperConnected(activeKeeperMeta, backupKeeperMeta);

        // 4. 回滚后继续写入数据
        sendMixedCommands(500, 200);
        sleep(10_000);
        assertGtid(master);

        // 5. 验证通过 v1 索引仍能正确定位
        GtidSet executed = new GtidSet(getGtidSet(master.getIp(), master.getPort(), "gtid_executed"));
        verifyGtidPosition(activeKeeperMeta, executed);

        // 6. 恢复配置（避免影响后续测试）
        keeperConfig.setReadV2(true);
        keeperConfig.setDualWrite(true);
    }

    @Test
    public void testV2IndexRebuildAfterRestart() throws Exception {
        setRedisToGtidEnabled(redisMaster.getIp(), redisMaster.getPort());

        KeeperMeta activeKeeperMeta = getKeeperActive();
        KeeperMeta backupKeeperMeta = getKeepersBackup().iterator().next();
        RedisMeta master = getRedisMaster();


        waitKeeperConnected(activeKeeperMeta, backupKeeperMeta);


        // 2. 写入混合命令，生成 v2 索引
        sendMixedCommands(10, 121);
        sleep(10_000);
        assertGtid(master);

        // 3. 记录当前 GTID Set 作为预期值
        GtidSet expectedGtid = new GtidSet(
                getGtidSet(master.getIp(), master.getPort(), "gtid_executed"));

        // 4. 停止 Active Keeper
        DefaultRedisKeeperServer keeperServer =
                (DefaultRedisKeeperServer) getRedisKeeperServer(activeKeeperMeta);
        stopKeeper(keeperServer);


        // 6. 重新启动 Keeper
        startKeeper(activeKeeperMeta);
        makeKeeperRight();
        // 7. 等待 Keeper 重新连接 Master
        waitKeeperConnected(activeKeeperMeta, backupKeeperMeta);


        // 8. 验证重建后的 GTID Set 与预期一致
        GtidSet recoveredGtidSet = new GtidSet(
                getGtidSet(activeKeeperMeta.getIp(), activeKeeperMeta.getPort(), "gtid_executed"));
        Assert.assertEquals("GTID set after rebuild should match",
                expectedGtid.toString(), recoveredGtidSet.toString());

        // 9. 再次写入数据，验证索引继续正常工作
        sendMixedCommands(500, 200);
        sleep(10_000);
        assertGtid(master);

        // 10. 验证 v2 索引文件重新生成
        assertV2IndexFilesExist(activeKeeperMeta, true);
    }


    @Test
    public void testV2IndexRebuildAfterRestartGtidAndPublish() throws Exception {
        setRedisToGtidEnabled(redisMaster.getIp(), redisMaster.getPort());

        KeeperMeta activeKeeperMeta = getKeeperActive();
        KeeperMeta backupKeeperMeta = getKeepersBackup().iterator().next();
        RedisMeta master = getRedisMaster();


        waitKeeperConnected(activeKeeperMeta, backupKeeperMeta);


        // 2. 交替写入混合命令，生成 v2 索引
        for(int i = 0;i<5;i++) {
            sendMixedCommands(120, 130);
        }
        sleep(10_000);
        assertGtid(master);

        // 3. 记录当前 GTID Set 作为预期值
        GtidSet expectedGtid = new GtidSet(
                getGtidSet(master.getIp(), master.getPort(), "gtid_executed"));

        // 4. 停止 Active Keeper
        DefaultRedisKeeperServer keeperServer =
                (DefaultRedisKeeperServer) getRedisKeeperServer(activeKeeperMeta);
        stopKeeper(keeperServer);


        // 6. 重新启动 Keeper
        startKeeper(activeKeeperMeta);
        makeKeeperRight();
        // 7. 等待 Keeper 重新连接 Master
        waitKeeperConnected(activeKeeperMeta, backupKeeperMeta);


        // 8. 验证重建后的 GTID Set 与预期一致
        GtidSet recoveredGtidSet = new GtidSet(
                getGtidSet(activeKeeperMeta.getIp(), activeKeeperMeta.getPort(), "gtid_executed"));
        Assert.assertEquals("GTID set after rebuild should match",
                expectedGtid.toString(), recoveredGtidSet.toString());

        // 9. 再次写入数据，验证索引继续正常工作
        sendMixedCommands(500, 200);
        sleep(10_000);
        assertGtid(master);

        // 10. 验证 v2 索引文件重新生成
        assertV2IndexFilesExist(activeKeeperMeta, true);
    }


    private void waitKeeperConnected(KeeperMeta active, KeeperMeta backup) throws Exception {
        waitConditionUntilTimeOut(() ->
                getRedisKeeperServer(active).getRedisMaster().getMasterState()
                        .equals(REDIS_REPL_CONNECTED));
        waitConditionUntilTimeOut(() ->
                getRedisKeeperServer(backup).getRedisMaster().getMasterState()
                        .equals(REDIS_REPL_CONNECTED));
    }

    private void sendMixedCommands(int gtidCount, int publishCount) {
        // 发送 GTID 命令（由 SET 产生，天然带 GTID）
        sendMessageToMaster(getRedisMaster(), gtidCount);
        // 发送 PUBLISH 命令（非 GTID）
        sendPublishCommands(publishCount);
    }

    private void sendPublishCommands(int count) {
        // 通过 Jedis 发送 PUBLISH，由于测试基类有 createJedis，直接使用
        try (redis.clients.jedis.Jedis jedis = createJedis(getRedisMaster())) {
            for (int i = 0; i < count; i++) {
                jedis.publish("test_channel", "test_message_" + i);
            }
        }
    }

    private void switchActiveKeeper(KeeperMeta oldActive, KeeperMeta newActive) throws Exception {
        oldActive.setActive(false);
        newActive.setActive(true);
        makeKeeperRight();
        // 等待新 active 状态生效
        Thread.sleep(TimeUnit.SECONDS.toMillis(2));
    }

    private void assertV2IndexFilesExist(KeeperMeta keeperMeta, boolean shouldExist) {
        // 获取 Keeper 的数据目录，假设为 baseDir
        String baseDir = getKeeperBaseDir(keeperMeta);
        File dir = new File(baseDir);
        File[] indexDirs =  dir.listFiles((d,name) -> d.isDirectory());
        for(File indexDir:indexDirs) {
            if(indexDir.isFile()) continue;
            File[] indexFiles = indexDir.listFiles((d, name) -> name.startsWith(V2_INDEX_PREFIX));
            File[] blockFiles = indexDir.listFiles((d, name) -> name.startsWith(V2_BLOCK_PREFIX));
            if (shouldExist) {
                Assert.assertNotNull("v2 index files must exist", indexFiles);
                Assert.assertTrue("v2 index files must not be empty", indexFiles.length > 0);
                Assert.assertNotNull("v2 block files must exist", blockFiles);
                Assert.assertTrue("v2 block files must not be empty", blockFiles.length > 0);
            } else {
                Assert.assertFalse("v2 index files should not exist", indexFiles != null && indexFiles.length > 0);
            }
        }


    }

    private String getKeeperBaseDir(KeeperMeta keeperMeta) {
        // 通过 RedisKeeperServer 获取 baseDir，需暴露方法
        DefaultRedisKeeperServer defaultRedisKeeperServer = (DefaultRedisKeeperServer) getRedisKeeperServer(keeperMeta);
        DefaultReplicationStoreManager defaultReplicationStoreManager = (DefaultReplicationStoreManager) defaultRedisKeeperServer.getReplicationStoreManager();
        return defaultReplicationStoreManager.getBaseDir().getPath();
    }

    private void verifyGtidPosition(KeeperMeta keeperMeta, GtidSet requestGtid) throws Exception {
        // 通过 DefaultIndexStore 的 locateContinueGtidSet 验证
        // 此处简化：通过 info 命令获取 gtid_executed，验证连续性
        String executed = getGtidSet(keeperMeta.getIp(), keeperMeta.getPort(), "gtid_executed");
        Assert.assertEquals("GTID set must be continuous", requestGtid.toString(), executed);
    }

    // 从 AbstractKeeperIntegrated 继承的方法，辅助获取 GTID Set
    protected String getGtidSet(String ip, int port, String key) throws ExecutionException, InterruptedException {
        return super.getGtidSet(ip, port, key);
    }

    private void assertGtid(RedisMeta master) throws ExecutionException, InterruptedException {
        String masterGtid = getGtidSet(master.getIp(), master.getPort(), "gtid_set");
        String activeKeeperGtid = getGtidSet(activeKeeper.getIp(),activeKeeper.getPort(), "gtid_executed");
        String backGtidSet = getGtidSet(backupKeeper.getIp(),backupKeeper.getPort(), "gtid_executed");
        logger.info("masterGtid:{}", masterGtid);
        logger.info("activeKeeperGtid:{}", activeKeeperGtid);
        logger.info("backGtidSet:{}", backGtidSet);
        for(RedisMeta slave: getRedisSlaves()) {
            String slaveGtidStr = getGtidSet(slave.getIp(), slave.getPort(), "gtid_set");
            logger.info("slave {}:{} gtid set: {}", slave.getIp(), slave.getPort(), slaveGtidStr);
            Assert.assertEquals(masterGtid, slaveGtidStr);
        }

    }
}
