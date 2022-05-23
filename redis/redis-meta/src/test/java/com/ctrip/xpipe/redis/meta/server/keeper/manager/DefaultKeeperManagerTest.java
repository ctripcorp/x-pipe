package com.ctrip.xpipe.redis.meta.server.keeper.manager;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.concurrent.KeyedOneThreadMutexableTaskExecutor;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.meta.server.keeper.manager.DefaultKeeperManager.ActiveKeeperInfoChecker;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.simpleserver.AbstractIoActionFactory;
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.OsUtils;
import com.google.common.collect.Lists;
import org.junit.*;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.*;

public class DefaultKeeperManagerTest extends AbstractTest {

    private DefaultKeeperManager manager = new DefaultKeeperManager();

    private CurrentMetaManager currentMetaManager;

    private SimpleKeyedObjectPool<Endpoint, NettyClient> pool;

    private ExecutorService executors;

    private DcMetaCache dcMetaCache;

    private String clusterId = "cluster", shardId = "shard";

    private Long clusterDbId = 1L, shardDbId = 1L;

    private KeyedOneThreadMutexableTaskExecutor keyedOneThreadMutexableTaskExecutor;

    @Before
    public void beforeDefaultKeeperManagerTest() throws Exception {
        currentMetaManager = mock(CurrentMetaManager.class);
        dcMetaCache = mock(DcMetaCache.class);
        manager.setCurrentMetaManager(currentMetaManager);
        manager.setMetaCache(dcMetaCache);
        pool = new XpipeNettyClientKeyedObjectPool();
        LifecycleHelper.initializeIfPossible(pool);
        LifecycleHelper.startIfPossible(pool);
        executors = Executors.newFixedThreadPool(OsUtils.getCpuCount());
        manager.setClientPool(pool);
        manager.setExecutors(executors);
        keyedOneThreadMutexableTaskExecutor = new KeyedOneThreadMutexableTaskExecutor<>(executors, scheduled);
        manager.setClusterShardExecutor(keyedOneThreadMutexableTaskExecutor);
    }

    @After
    public void afterDefaultKeeperManagerTest() {
        executors.shutdownNow();
    }


    @Test
    public void testActiveKeeperChecker() {
        Pair<String, Integer> keeperMaster = new Pair<>("localhost", randomInt());
        InfoResultExtractor extractor = mock(InfoResultExtractor.class);
        when(extractor.extract("state")).thenReturn("ACTIVE");
        when(extractor.extract("master_host")).thenReturn("localhost");
        when(extractor.extract("master_port")).thenReturn(String.valueOf(keeperMaster.getValue()));
        when(currentMetaManager.getKeeperMaster(clusterDbId, shardDbId)).thenReturn(keeperMaster);
        ActiveKeeperInfoChecker checker = spy(manager.new ActiveKeeperInfoChecker(extractor, clusterDbId, shardDbId));
        Assert.assertTrue(checker.isValid());
    }

    @Test
    public void testBackupKeeperChecker() {
        KeeperMeta keeperActive = new KeeperMeta().setActive(true).setIp("localhost").setPort(randomInt());
        InfoResultExtractor extractor = mock(InfoResultExtractor.class);
        when(extractor.extract("state")).thenReturn("BACKUP");
        when(extractor.extract("master_host")).thenReturn(keeperActive.getIp());
        when(extractor.extract("master_port")).thenReturn(String.valueOf(keeperActive.getPort()));
        when(currentMetaManager.getKeeperActive(clusterDbId, shardDbId)).thenReturn(keeperActive);
        DefaultKeeperManager.BackupKeeperInfoChecker checker = spy(manager.new BackupKeeperInfoChecker(extractor, clusterDbId, shardDbId));
        Assert.assertTrue(checker.isValid());
    }

    @Test
    public void testActiveKeeperCheckerDoCorrect() {
        Pair<String, Integer> keeperMaster = new Pair<>("localhost", randomInt());
        InfoResultExtractor extractor = mock(InfoResultExtractor.class);
        when(extractor.extract("state")).thenReturn("BACKUP");
        when(extractor.extract("master_host")).thenReturn("localhost");
        when(extractor.extract("master_port")).thenReturn(String.valueOf(keeperMaster.getValue()));
        when(currentMetaManager.getKeeperMaster(clusterDbId, shardDbId)).thenReturn(keeperMaster);
        ActiveKeeperInfoChecker checker = spy(manager.new ActiveKeeperInfoChecker(extractor, clusterDbId, shardDbId));
        Assert.assertFalse(checker.isValid());
        when(extractor.extract("master_host")).thenReturn("localhost");
        when(extractor.extract("master_port")).thenReturn(String.valueOf(randomInt()));
        Assert.assertFalse(checker.isValid());
    }

    @Test
    public void testBackupKeeperCheckerDoCorrect() {
        KeeperMeta keeperActive = new KeeperMeta().setActive(true).setIp("localhost").setPort(randomInt());
        InfoResultExtractor extractor = mock(InfoResultExtractor.class);
        when(extractor.extract("state")).thenReturn("ACTIVE");
        when(extractor.extract("master_host")).thenReturn(keeperActive.getIp());
        when(extractor.extract("master_port")).thenReturn(String.valueOf(keeperActive.getPort()));
        when(currentMetaManager.getKeeperActive(clusterDbId, shardDbId)).thenReturn(keeperActive);
        DefaultKeeperManager.BackupKeeperInfoChecker checker = spy(manager.new BackupKeeperInfoChecker(extractor, clusterDbId, shardDbId));
        Assert.assertFalse(checker.isValid());
    }


    //manually integration test

    @Test
    public void integrateTest() throws Exception {
        String clusterId = "clsuter", shardId = "shard";
        AtomicInteger infoCount = new AtomicInteger(0);
        AtomicInteger keeperCommandCounter = new AtomicInteger(0);
        int masterPort = randomInt();
        Server activeKeeper = startServer(new AbstractIoActionFactory() {
            @Override
            protected byte[] getToWrite(Object readResult) {
                String input = (String) readResult;
                if(input.trim().toLowerCase().startsWith("info")) {
                    infoCount.incrementAndGet();
                    return ("+state:ACTIVE\nmaster_host:localhost\nmaster_port:"+masterPort+"\r\n").getBytes();
                } else if(input.trim().toLowerCase().startsWith("keeper")) {
                    keeperCommandCounter.incrementAndGet();
                    return "+OK\r\n".getBytes();
                }
                return new byte[0];
            }
        });

        int activeKeeperPort = activeKeeper.getPort();
        Server backupKeeper = startServer(new AbstractIoActionFactory() {
            @Override
            protected byte[] getToWrite(Object readResult) {
                String input = (String) readResult;
                if(input.trim().equalsIgnoreCase("info replication")) {
                    infoCount.incrementAndGet();
                    return ("+state: BACKUP\nmaster_host:localhost\nmaster_port:"+activeKeeperPort+"\r\n").getBytes();
                } else if(input.trim().toLowerCase().startsWith("keeper")) {
                    keeperCommandCounter.incrementAndGet();
                    return "+OK\r\n".getBytes();
                }
                return new byte[0];
            }
        });

        when(currentMetaManager.getKeeperActive(clusterDbId, shardDbId))
                .thenReturn(new KeeperMeta().setActive(true).setIp("localhost").setPort(activeKeeperPort));
        when(currentMetaManager.getKeeperMaster(clusterDbId, shardDbId)).thenReturn(new Pair<>("localhost", masterPort));
        when(currentMetaManager.getSurviveKeepers(clusterDbId, shardDbId)).thenReturn(Lists.newArrayList(
                new KeeperMeta().setActive(true).setIp("localhost").setPort(activeKeeperPort),
                new KeeperMeta().setActive(false).setIp("localhost").setPort(backupKeeper.getPort())
        ));
//        sleep(1000);
        DefaultKeeperManager.KeeperStateAlignChecker checker = manager.new KeeperStateAlignChecker();
        checker.doCheckShard(new ClusterMeta(clusterId).setDbId(clusterDbId), new ShardMeta().setId(shardId).setDbId(shardDbId));
        sleep(1500);
        Assert.assertEquals(2, infoCount.get());
        Assert.assertEquals(0, keeperCommandCounter.get());

        activeKeeper.stop();
        backupKeeper.stop();
    }


    @Test
    public void integrateTestDoCorrect() throws Exception {
        AtomicInteger infoCount = new AtomicInteger(0);
        AtomicInteger keeperCommandCounter = new AtomicInteger(0);
        int masterPort = randomInt();
        Server activeKeeper = startServer(new AbstractIoActionFactory() {
            @Override
            protected byte[] getToWrite(Object readResult) {
                String input = (String) readResult;
                if(input.trim().toLowerCase().startsWith("info")) {
                    infoCount.incrementAndGet();
                    return ("+state:BACKUP\nmaster_host:localhost\nmaster_port:"+masterPort+"\r\n").getBytes();
                } else if(input.trim().toLowerCase().startsWith("keeper")) {
                    keeperCommandCounter.incrementAndGet();
                    return "+OK\r\n".getBytes();
                }
                return new byte[0];
            }
        });

        int activeKeeperPort = activeKeeper.getPort();
        Server backupKeeper = startServer(new AbstractIoActionFactory() {
            @Override
            protected byte[] getToWrite(Object readResult) {
                String input = (String) readResult;
                if(input.trim().equalsIgnoreCase("info replication")) {
                    infoCount.incrementAndGet();
                    return ("+state: BACKUP\nmaster_host:localhost\nmaster_port:"+activeKeeperPort+"\r\n").getBytes();
                } else if(input.trim().toLowerCase().startsWith("keeper")) {
                    keeperCommandCounter.incrementAndGet();
                    return "+OK\r\n".getBytes();
                }
                return new byte[0];
            }
        });

        when(currentMetaManager.getKeeperActive(clusterDbId, shardDbId))
                .thenReturn(new KeeperMeta().setActive(true).setIp("localhost").setPort(activeKeeperPort));
        when(currentMetaManager.getClusterMeta(clusterDbId)).thenReturn(new ClusterMeta().setActiveDc("jq"));
        when(currentMetaManager.getKeeperMaster(clusterDbId, shardDbId)).thenReturn(new Pair<>("localhost", masterPort));
        when(currentMetaManager.getSurviveKeepers(clusterDbId, shardDbId)).thenReturn(Lists.newArrayList(
                new KeeperMeta().setActive(true).setIp("localhost").setPort(activeKeeperPort),
                new KeeperMeta().setActive(false).setIp("localhost").setPort(backupKeeper.getPort())
        ));
        when(dcMetaCache.getShardRedises(clusterDbId, shardDbId)).thenReturn(Lists.newArrayList(
                new RedisMeta().setIp("localhost").setPort(masterPort),
                new RedisMeta().setIp("localhost").setPort(randomPort())
        ));
        when(dcMetaCache.isCurrentDcPrimary(anyLong())).thenReturn(true);
        DefaultKeeperManager.KeeperStateAlignChecker checker = manager.new KeeperStateAlignChecker();
        checker.doCheckShard(new ClusterMeta(clusterId).setDbId(clusterDbId), new ShardMeta().setId(shardId).setDbId(shardDbId));
        waitConditionUntilTimeOut(()->infoCount.get() > 0, 2000);
        sleep(200);
        Assert.assertEquals(1, infoCount.get());
        Assert.assertEquals(2, keeperCommandCounter.get());

        activeKeeper.stop();
        backupKeeper.stop();
    }

    @Test
    public void integrateTestDoCorrect2() throws Exception {
        AtomicInteger infoCount = new AtomicInteger(0);
        AtomicInteger keeperCommandCounter = new AtomicInteger(0);
        int masterPort = randomInt();
        Server activeKeeper = startServer(new AbstractIoActionFactory() {
            @Override
            protected byte[] getToWrite(Object readResult) {
                String input = (String) readResult;
                if(input.trim().toLowerCase().startsWith("info")) {
                    infoCount.incrementAndGet();
                    return ("+state:ACTIVE\nmaster_host:localhost\nmaster_port:"+randomInt()+"\r\n").getBytes();
                } else if(input.trim().toLowerCase().startsWith("keeper")) {
                    keeperCommandCounter.incrementAndGet();
                    return "+OK\r\n".getBytes();
                }
                return new byte[0];
            }
        });

        int activeKeeperPort = activeKeeper.getPort();
        Server backupKeeper = startServer(new AbstractIoActionFactory() {
            @Override
            protected byte[] getToWrite(Object readResult) {
                String input = (String) readResult;
                if(input.trim().equalsIgnoreCase("info replication")) {
                    infoCount.incrementAndGet();
                    return ("+state: BACKUP\nmaster_host:localhost\nmaster_port:"+activeKeeperPort+"\r\n").getBytes();
                } else if(input.trim().toLowerCase().startsWith("keeper")) {
                    keeperCommandCounter.incrementAndGet();
                    return "+OK\r\n".getBytes();
                }
                return new byte[0];
            }
        });

        when(currentMetaManager.getKeeperActive(clusterDbId, shardDbId))
                .thenReturn(new KeeperMeta().setActive(true).setIp("localhost").setPort(activeKeeperPort));
        when(currentMetaManager.getClusterMeta(clusterDbId)).thenReturn(new ClusterMeta().setActiveDc("jq"));
        when(currentMetaManager.getKeeperMaster(clusterDbId, shardDbId)).thenReturn(new Pair<>("localhost", masterPort));
        when(currentMetaManager.getSurviveKeepers(clusterDbId, shardDbId)).thenReturn(Lists.newArrayList(
                new KeeperMeta().setActive(true).setIp("localhost").setPort(activeKeeperPort),
                new KeeperMeta().setActive(false).setIp("localhost").setPort(backupKeeper.getPort())
        ));
        when(dcMetaCache.getShardRedises(clusterDbId, shardDbId)).thenReturn(Lists.newArrayList(
                new RedisMeta().setIp("localhost").setPort(masterPort),
                new RedisMeta().setIp("localhost").setPort(randomPort())
        ));
        when(dcMetaCache.isCurrentDcPrimary(anyLong())).thenReturn(true);

        DefaultKeeperManager.KeeperStateAlignChecker checker = manager.new KeeperStateAlignChecker();
        checker.doCheckShard(new ClusterMeta(clusterId).setDbId(clusterDbId), new ShardMeta().setId(shardId).setDbId(shardDbId));
        waitConditionUntilTimeOut(()->infoCount.get() > 0, 2000);
        sleep(200);
        Assert.assertEquals(1, infoCount.get());
        Assert.assertEquals(2, keeperCommandCounter.get());

        activeKeeper.stop();
        backupKeeper.stop();
    }

    @Test
    public void testWhenMigratingPrimaryDc() throws Exception {
        AtomicInteger infoCount = new AtomicInteger(0);
        AtomicInteger keeperCommandCounter = new AtomicInteger(0);
        int masterPort = randomInt(), backupSiteActiveKeeperPort = randomInt();
        Server activeKeeper = startServer(new AbstractIoActionFactory() {
            @Override
            protected byte[] getToWrite(Object readResult) {
                String input = (String) readResult;
                if(input.trim().toLowerCase().startsWith("info")) {
                    infoCount.incrementAndGet();
                    return ("+state:ACTIVE\nmaster_host:localhost\nmaster_port:"+masterPort+"\r\n").getBytes();
                } else if(input.trim().toLowerCase().startsWith("keeper")) {
                    keeperCommandCounter.incrementAndGet();
                    return "+OK\r\n".getBytes();
                }
                return new byte[0];
            }
        });

        int activeKeeperPort = activeKeeper.getPort();
        Server backupKeeper = startServer(new AbstractIoActionFactory() {
            @Override
            protected byte[] getToWrite(Object readResult) {
                String input = (String) readResult;
                if(input.trim().equalsIgnoreCase("info replication")) {
                    infoCount.incrementAndGet();
                    return ("+state: BACKUP\nmaster_host:localhost\nmaster_port:"+activeKeeperPort+"\r\n").getBytes();
                } else if(input.trim().toLowerCase().startsWith("keeper")) {
                    keeperCommandCounter.incrementAndGet();
                    return "+OK\r\n".getBytes();
                }
                return new byte[0];
            }
        });

        when(currentMetaManager.getKeeperActive(clusterDbId, shardDbId))
                .thenReturn(new KeeperMeta().setActive(true).setIp("localhost").setPort(activeKeeperPort));
        when(currentMetaManager.getKeeperMaster(clusterDbId, shardDbId)).thenReturn(new Pair<>("localhost", backupSiteActiveKeeperPort));
        when(currentMetaManager.getSurviveKeepers(clusterDbId, shardDbId)).thenReturn(Lists.newArrayList(
                new KeeperMeta().setActive(true).setIp("localhost").setPort(activeKeeperPort),
                new KeeperMeta().setActive(false).setIp("localhost").setPort(backupKeeper.getPort())
        ));
        when(dcMetaCache.isCurrentDcPrimary(anyLong())).thenReturn(true);
//        sleep(1000);
        DefaultKeeperManager.KeeperStateAlignChecker checker = manager.new KeeperStateAlignChecker();
        checker.doCheckShard(new ClusterMeta(clusterId).setDbId(clusterDbId), new ShardMeta().setId(shardId).setDbId(shardDbId));
        waitConditionUntilTimeOut(()->infoCount.get() > 0, 2000);
        sleep(200);
        Assert.assertEquals(2, infoCount.get());
        Assert.assertEquals(0, keeperCommandCounter.get());

        activeKeeper.stop();
        backupKeeper.stop();
    }

    @Test
    public void testWhenMigratingOtherDc() throws Exception {
        String clusterId = "clsuter", shardId = "shard";
        AtomicInteger infoCount = new AtomicInteger(0);
        AtomicInteger keeperCommandCounter = new AtomicInteger(0);
        int redisPort = randomInt(), primarySiteActiveKeeperPort = randomInt();
        Server activeKeeper = startServer(new AbstractIoActionFactory() {
            @Override
            protected byte[] getToWrite(Object readResult) {
                String input = (String) readResult;
                if(input.trim().toLowerCase().startsWith("info")) {
                    infoCount.incrementAndGet();
                    return ("+state:ACTIVE\nmaster_host:localhost\nmaster_port:"+primarySiteActiveKeeperPort+"\r\n").getBytes();
                } else if(input.trim().toLowerCase().startsWith("keeper")) {
                    keeperCommandCounter.incrementAndGet();
                    return "+OK\r\n".getBytes();
                }
                return new byte[0];
            }
        });

        int activeKeeperPort = activeKeeper.getPort();
        Server backupKeeper = startServer(new AbstractIoActionFactory() {
            @Override
            protected byte[] getToWrite(Object readResult) {
                String input = (String) readResult;
                if(input.trim().equalsIgnoreCase("info replication")) {
                    infoCount.incrementAndGet();
                    return ("+state: BACKUP\nmaster_host:localhost\nmaster_port:"+activeKeeperPort+"\r\n").getBytes();
                } else if(input.trim().toLowerCase().startsWith("keeper")) {
                    keeperCommandCounter.incrementAndGet();
                    return "+OK\r\n".getBytes();
                }
                return new byte[0];
            }
        });

        when(currentMetaManager.getKeeperActive(clusterDbId, shardDbId))
                .thenReturn(new KeeperMeta().setActive(true).setIp("localhost").setPort(activeKeeperPort));
        when(currentMetaManager.getKeeperMaster(clusterDbId, shardDbId)).thenReturn(new Pair<>("localhost", redisPort));
        when(currentMetaManager.getSurviveKeepers(clusterDbId, shardDbId)).thenReturn(Lists.newArrayList(
                new KeeperMeta().setActive(true).setIp("localhost").setPort(activeKeeperPort),
                new KeeperMeta().setActive(false).setIp("localhost").setPort(backupKeeper.getPort())
        ));
        when(dcMetaCache.isCurrentDcPrimary(anyLong())).thenReturn(true);
//        sleep(1000);
        DefaultKeeperManager.KeeperStateAlignChecker checker = manager.new KeeperStateAlignChecker();
        checker.doCheckShard(new ClusterMeta(clusterId).setDbId(clusterDbId), new ShardMeta().setId(shardId).setDbId(shardDbId));
        waitConditionUntilTimeOut(()->infoCount.get() > 0, 2000);
        sleep(200);
        Assert.assertEquals(2, infoCount.get());
        Assert.assertEquals(0, keeperCommandCounter.get());

        activeKeeper.stop();
        backupKeeper.stop();
    }

    //manual test
    @Ignore
    @Test
    public void testCancelExecution() throws Exception {
        String clusterId = "clsuter", shardId = "shard";
        AtomicInteger infoCount = new AtomicInteger(0);
        AtomicInteger keeperCommandCounter = new AtomicInteger(0);
        int masterPort = randomInt();
        Server activeKeeper = startServer(new AbstractIoActionFactory() {
            @Override
            protected byte[] getToWrite(Object readResult) {
                String input = (String) readResult;
                if(input.trim().toLowerCase().startsWith("info")) {
                    infoCount.incrementAndGet();
                    return ("+state:ACTIVE\nmaster_host:localhost\nmaster_port:"+randomInt()+"\r\n").getBytes();
                } else if(input.trim().toLowerCase().startsWith("keeper")) {
                    keeperCommandCounter.incrementAndGet();
                    return "+OK\r\n".getBytes();
                }
                return new byte[0];
            }
        });

        int activeKeeperPort = activeKeeper.getPort();
        Server backupKeeper = startServer(new AbstractIoActionFactory() {
            @Override
            protected byte[] getToWrite(Object readResult) {
                String input = (String) readResult;
                if(input.trim().equalsIgnoreCase("info replication")) {
                    infoCount.incrementAndGet();
                    return ("+state: BACKUP\nmaster_host:localhost\nmaster_port:"+activeKeeperPort+"\r\n").getBytes();
                } else if(input.trim().toLowerCase().startsWith("keeper")) {
                    keeperCommandCounter.incrementAndGet();
                    return "+OK\r\n".getBytes();
                }
                return new byte[0];
            }
        });

        when(currentMetaManager.getKeeperActive(clusterDbId, shardDbId))
                .thenReturn(new KeeperMeta().setActive(true).setIp("localhost").setPort(activeKeeperPort));
        when(currentMetaManager.getKeeperMaster(clusterDbId, shardDbId)).thenReturn(new Pair<>("localhost", masterPort));
        when(currentMetaManager.getSurviveKeepers(clusterDbId, shardDbId)).thenReturn(Lists.newArrayList(
                new KeeperMeta().setActive(true).setIp("localhost").setPort(activeKeeperPort),
                new KeeperMeta().setActive(false).setIp("localhost").setPort(backupKeeper.getPort())
        ));
        when(dcMetaCache.getShardRedises(clusterDbId, shardDbId)).thenReturn(Lists.newArrayList(
                new RedisMeta().setIp("localhost").setPort(masterPort),
                new RedisMeta().setIp("localhost").setPort(randomPort())
        ));
        when(dcMetaCache.isCurrentDcPrimary(anyLong())).thenReturn(true);

        DefaultKeeperManager.KeeperStateAlignChecker checker = manager.new KeeperStateAlignChecker();
        checker.doCheckShard(new ClusterMeta(clusterId).setDbId(1L), new ShardMeta().setId(shardId));
        executors.execute(new Runnable() {
            @Override
            public void run() {
                for(int i = 0; i < 100; i++) {
                    keyedOneThreadMutexableTaskExecutor.clearAndExecute(Pair.from(clusterDbId, shardDbId),
                            new CountingCommand(new AtomicInteger(), 10));
                    sleep(1);
                }
            }
        });
        waitConditionUntilTimeOut(()->infoCount.get() > 0, 2000);

        sleep(200);
        Assert.assertEquals(1, infoCount.get());
        Assert.assertEquals(0, keeperCommandCounter.get());

        activeKeeper.stop();
        backupKeeper.stop();
    }

    @Test
    public void timeoutNotDoCorrectTest() throws Exception {
        String clusterId = "clsuter", shardId = "shard";
        final CountDownLatch latch = new CountDownLatch(2);

        Server timeoutKeeper = startServer(new AbstractIoActionFactory() {
            @Override
            protected byte[] getToWrite(Object readResult) {
                try {
                    sleep(10);
                } catch (Exception e) {
                    // do nothing
                }

                latch.countDown();
                return ("+state:ACTIVE\nmaster_host:localhost\nmaster_port:"+randomInt()+"\r\n").getBytes();
            }
        });
        Server normalKeeper = startServer(new AbstractIoActionFactory() {
            @Override
            protected byte[] getToWrite(Object readResult) {
                latch.countDown();
                return ("+state: BACKUP\nmaster_host:localhost\nmaster_port:"+timeoutKeeper.getPort()+"\r\n").getBytes();
            }
        });

        manager.setScheduled(scheduled);
        KeeperMeta timeoutKeeperMeta = new KeeperMeta().setIp("localhost").setPort(timeoutKeeper.getPort()).setActive(false);
        KeeperMeta normalKeeperMeta = new KeeperMeta().setIp("localhost").setPort(normalKeeper.getPort()).setActive(false);
        when(currentMetaManager.getSurviveKeepers(clusterDbId, shardDbId))
                .thenReturn(Arrays.asList(timeoutKeeperMeta, normalKeeperMeta));

        int originTimeout = InfoCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI;

        try {
            InfoCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI = 1;

            DefaultKeeperManager.KeeperStateAlignChecker checker = spy(manager.new KeeperStateAlignChecker());
            doNothing().when(checker).doCorrect(anyLong(), anyLong(), anyList());
            checker.doCheckShard(new ClusterMeta(clusterId).setDbId(1L), new ShardMeta().setId(shardId));

            latch.await(1000, TimeUnit.MILLISECONDS);
            sleep(100);
            verify(checker, never()).doCorrect(anyLong(), anyLong(), anyList());

            timeoutKeeper.stop();
            normalKeeper.stop();
        } finally {
            InfoCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI = originTimeout;
        }
    }
}