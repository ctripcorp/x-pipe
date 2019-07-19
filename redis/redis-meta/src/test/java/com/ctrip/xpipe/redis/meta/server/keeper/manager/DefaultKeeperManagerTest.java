package com.ctrip.xpipe.redis.meta.server.keeper.manager;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.meta.server.keeper.manager.DefaultKeeperManager.ActiveKeeperInfoChecker;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.simpleserver.AbstractIoActionFactory;
import com.ctrip.xpipe.simpleserver.IoAction;
import com.ctrip.xpipe.simpleserver.IoActionFactory;
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import org.junit.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.*;

public class DefaultKeeperManagerTest extends AbstractTest {

    private DefaultKeeperManager manager = new DefaultKeeperManager();

    private CurrentMetaManager currentMetaManager;

    private SimpleKeyedObjectPool<Endpoint, NettyClient> pool;

    private ExecutorService executors;

    @Before
    public void beforeDefaultKeeperManagerTest() throws Exception {
        currentMetaManager = mock(CurrentMetaManager.class);
        manager.setCurrentMetaManager(currentMetaManager);
        pool = new XpipeNettyClientKeyedObjectPool();
        LifecycleHelper.initializeIfPossible(pool);
        LifecycleHelper.startIfPossible(pool);
        executors = Executors.newSingleThreadExecutor();
        manager.setClientPool(pool);
        manager.setExecutors(executors);
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
        when(currentMetaManager.getKeeperMaster("cluster", "shard")).thenReturn(keeperMaster);
        ActiveKeeperInfoChecker checker = spy(manager.new ActiveKeeperInfoChecker(extractor, "cluster", "shard"));
        Assert.assertTrue(checker.isValid());
    }

    @Test
    public void testBackupKeeperChecker() {
        KeeperMeta keeperActive = new KeeperMeta().setActive(true).setIp("localhost").setPort(randomInt());
        InfoResultExtractor extractor = mock(InfoResultExtractor.class);
        when(extractor.extract("state")).thenReturn("BACKUP");
        when(extractor.extract("master_host")).thenReturn(keeperActive.getIp());
        when(extractor.extract("master_port")).thenReturn(String.valueOf(keeperActive.getPort()));
        when(currentMetaManager.getKeeperActive("cluster", "shard")).thenReturn(keeperActive);
        DefaultKeeperManager.BackupKeeperInfoChecker checker = spy(manager.new BackupKeeperInfoChecker(extractor, "cluster", "shard"));
        Assert.assertTrue(checker.isValid());
    }

    @Test
    public void testActiveKeeperCheckerDoCorrect() {
        Pair<String, Integer> keeperMaster = new Pair<>("localhost", randomInt());
        InfoResultExtractor extractor = mock(InfoResultExtractor.class);
        when(extractor.extract("state")).thenReturn("BACKUP");
        when(extractor.extract("master_host")).thenReturn("localhost");
        when(extractor.extract("master_port")).thenReturn(String.valueOf(keeperMaster.getValue()));
        when(currentMetaManager.getKeeperMaster("cluster", "shard")).thenReturn(keeperMaster);
        ActiveKeeperInfoChecker checker = spy(manager.new ActiveKeeperInfoChecker(extractor, "cluster", "shard"));
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
        when(currentMetaManager.getKeeperActive("cluster", "shard")).thenReturn(keeperActive);
        DefaultKeeperManager.BackupKeeperInfoChecker checker = spy(manager.new BackupKeeperInfoChecker(extractor, "cluster", "shard"));
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

        when(currentMetaManager.getKeeperActive(clusterId, shardId))
                .thenReturn(new KeeperMeta().setActive(true).setIp("localhost").setPort(activeKeeperPort));
        when(currentMetaManager.getKeeperMaster(clusterId, shardId)).thenReturn(new Pair<>("localhost", masterPort));
        when(currentMetaManager.getSurviveKeepers(clusterId, shardId)).thenReturn(Lists.newArrayList(
                new KeeperMeta().setActive(true).setIp("localhost").setPort(activeKeeperPort),
                new KeeperMeta().setActive(false).setIp("localhost").setPort(backupKeeper.getPort())
        ));
//        sleep(1000);
        DefaultKeeperManager.KeeperStateAlignChecker checker = manager.new KeeperStateAlignChecker();
        checker.doCheckShard(clusterId, new ShardMeta().setId(shardId));
        sleep(1500);
        Assert.assertEquals(2, infoCount.get());
        Assert.assertEquals(0, keeperCommandCounter.get());

        activeKeeper.stop();
        backupKeeper.stop();
    }


    @Test
    public void integrateTestDoCorrect() throws Exception {
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

        when(currentMetaManager.getKeeperActive(clusterId, shardId))
                .thenReturn(new KeeperMeta().setActive(true).setIp("localhost").setPort(activeKeeperPort));
        when(currentMetaManager.getKeeperMaster(clusterId, shardId)).thenReturn(new Pair<>("localhost", masterPort));
        when(currentMetaManager.getSurviveKeepers(clusterId, shardId)).thenReturn(Lists.newArrayList(
                new KeeperMeta().setActive(true).setIp("localhost").setPort(activeKeeperPort),
                new KeeperMeta().setActive(false).setIp("localhost").setPort(backupKeeper.getPort())
        ));
//        sleep(1000);
        DefaultKeeperManager.KeeperStateAlignChecker checker = manager.new KeeperStateAlignChecker();
        checker.doCheckShard(clusterId, new ShardMeta().setId(shardId));
        sleep(1500);
        Assert.assertEquals(1, infoCount.get());
        Assert.assertEquals(2, keeperCommandCounter.get());

        activeKeeper.stop();
        backupKeeper.stop();
    }

    @Test
    public void integrateTestDoCorrect2() throws Exception {
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

        when(currentMetaManager.getKeeperActive(clusterId, shardId))
                .thenReturn(new KeeperMeta().setActive(true).setIp("localhost").setPort(activeKeeperPort));
        when(currentMetaManager.getKeeperMaster(clusterId, shardId)).thenReturn(new Pair<>("localhost", masterPort));
        when(currentMetaManager.getSurviveKeepers(clusterId, shardId)).thenReturn(Lists.newArrayList(
                new KeeperMeta().setActive(true).setIp("localhost").setPort(activeKeeperPort),
                new KeeperMeta().setActive(false).setIp("localhost").setPort(backupKeeper.getPort())
        ));
//        sleep(1000);
        DefaultKeeperManager.KeeperStateAlignChecker checker = manager.new KeeperStateAlignChecker();
        checker.doCheckShard(clusterId, new ShardMeta().setId(shardId));
        sleep(1500);
        Assert.assertEquals(1, infoCount.get());
        Assert.assertEquals(2, keeperCommandCounter.get());

        activeKeeper.stop();
        backupKeeper.stop();
    }
}