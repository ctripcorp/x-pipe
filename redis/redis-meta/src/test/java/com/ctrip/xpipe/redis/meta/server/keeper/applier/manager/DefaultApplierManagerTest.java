package com.ctrip.xpipe.redis.meta.server.keeper.applier.manager;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaClone;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.meta.server.keeper.applier.ApplierStateController;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.OsUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.*;

/**
 * @author ayq
 * <p>
 * 2022/4/7 11:42
 */
public class DefaultApplierManagerTest extends AbstractTest {

    private DefaultApplierManager manager = new DefaultApplierManager();

    private CurrentMetaManager currentMetaManager;

    private SimpleKeyedObjectPool<Endpoint, NettyClient> pool;

    private ExecutorService executors;

    private String clusterId = "cluster", shardId = "shard";

    private Long clusterDbId = 1L, shardDbId = 1L;

    @Before
    public void beforeDefaultApplierManagerTest() throws Exception {
        currentMetaManager = mock(CurrentMetaManager.class);
        manager.setCurrentMetaManager(currentMetaManager);
        pool = new XpipeNettyClientKeyedObjectPool();
        LifecycleHelper.initializeIfPossible(pool);
        LifecycleHelper.startIfPossible(pool);
        executors = Executors.newFixedThreadPool(OsUtils.getCpuCount());
        manager.setExecutors(executors);
    }

    @After
    public void afterDefaultKeeperManagerTest() {
        executors.shutdownNow();
    }

    @Test
    public void testActiveApplierCheckerValid() {
        Pair<String, Integer> applierMaster = new Pair<>("localhost", randomInt());
        InfoResultExtractor extractor = mock(InfoResultExtractor.class);
        when(extractor.extract("state")).thenReturn("ACTIVE");
        when(extractor.extract("master_host")).thenReturn("localhost");
        when(extractor.extract("master_port")).thenReturn(String.valueOf(applierMaster.getValue()));
        when(currentMetaManager.getApplierMaster(clusterDbId, shardDbId)).thenReturn(applierMaster);
        DefaultApplierManager.ActiveApplierInfoChecker checker = spy(manager.new ActiveApplierInfoChecker(extractor, clusterDbId, shardDbId));

        Assert.assertTrue(checker.isValid());
    }

    @Test
    public void testActiveApplierCheckerInvalid() {
        InfoResultExtractor extractor = mock(InfoResultExtractor.class);
        when(extractor.extract("state")).thenReturn("ACTIVE");
        when(currentMetaManager.getApplierMaster(clusterDbId, shardDbId)).thenReturn(null);
        DefaultApplierManager.ActiveApplierInfoChecker checker = spy(manager.new ActiveApplierInfoChecker(extractor, clusterDbId, shardDbId));

        Assert.assertFalse(checker.isValid());
    }

    @Test
    public void testBackupApplierCheckerValid() {
        InfoResultExtractor extractor = mock(InfoResultExtractor.class);
        when(extractor.extract("state")).thenReturn("BACKUP");
        when(currentMetaManager.getApplierMaster(clusterDbId, shardDbId)).thenReturn(null);
        DefaultApplierManager.BackupApplierInfoChecker checker = spy(manager.new BackupApplierInfoChecker(extractor, clusterDbId, shardDbId));

        Assert.assertTrue(checker.isValid());
    }

    @Test
    public void testBackupApplierCheckerInvalid() {
        Pair<String, Integer> applierMaster = new Pair<>("localhost", randomInt());
        InfoResultExtractor extractor = mock(InfoResultExtractor.class);
        when(extractor.extract("state")).thenReturn("BACKUP");
        when(extractor.extract("master_host")).thenReturn("localhost");
        when(extractor.extract("master_port")).thenReturn(String.valueOf(applierMaster.getValue()));
        when(currentMetaManager.getApplierMaster(clusterDbId, shardDbId)).thenReturn(applierMaster);
        DefaultApplierManager.BackupApplierInfoChecker checker = spy(manager.new BackupApplierInfoChecker(extractor, clusterDbId, shardDbId));

        Assert.assertTrue(!checker.isValid());
    }

    @Test
    public void testHandleClusterAdded() {
        ApplierMeta applierMeta = new ApplierMeta();
        SourceMeta sourceMeta = new SourceMeta();
        ShardMeta shardMeta = new ShardMeta();
        ClusterMeta clusterMeta = new ClusterMeta();

        shardMeta.setDbId(shardDbId);
        clusterMeta.setDbId(clusterDbId);
        shardMeta.addApplier(applierMeta);
        sourceMeta.addShard(shardMeta);
        clusterMeta.addSource(sourceMeta);

        DefaultApplierManager defaultApplierManager = new DefaultApplierManager();
        ApplierStateController applierStateController = mock(ApplierStateController.class);
        defaultApplierManager.setApplierStateController(applierStateController);
        ApplierTransMeta applierTransMeta = new ApplierTransMeta(clusterDbId, shardDbId, applierMeta);

        defaultApplierManager.handleClusterAdd(clusterMeta);
        doNothing().when(applierStateController).addApplier(applierTransMeta);
        verify(applierStateController, times(1)).addApplier(applierTransMeta);
    }

    @Test
    public void testHandleClusterRemoved() {
        ClusterMeta clusterMeta = buildClusterMeta();

        DefaultApplierManager defaultApplierManager = new DefaultApplierManager();
        ApplierStateController applierStateController = mock(ApplierStateController.class);
        defaultApplierManager.setApplierStateController(applierStateController);

        defaultApplierManager.handleClusterDeleted(clusterMeta);

        doNothing().when(applierStateController).removeApplier(any());
        verify(applierStateController, times(1)).removeApplier(any());
    }

    @Test
    public void testHandleClusterModified() {

        ClusterMeta current = buildClusterMeta();
        ClusterMeta future = MetaClone.clone(current);
        ApplierMeta applierMeta2 = new ApplierMeta();
        applierMeta2.setIp("applier2");
        future.getSources().get(0).getShards().get(shardId).getAppliers().remove(0);
        future.getSources().get(0).getShards().get(shardId).addApplier(applierMeta2);

        ClusterMetaComparator comparator = new ClusterMetaComparator(current, future);
        comparator.compare();

        DefaultApplierManager defaultApplierManager = new DefaultApplierManager();
        ApplierStateController applierStateController = mock(ApplierStateController.class);
        defaultApplierManager.setApplierStateController(applierStateController);
        defaultApplierManager.handleClusterModified(comparator);

        doNothing().when(applierStateController).addApplier(any());
        verify(applierStateController, times(1)).addApplier(any());

        doNothing().when(applierStateController).removeApplier(any());
        verify(applierStateController, times(1)).removeApplier(any());
    }

    private ClusterMeta buildClusterMeta() {

        ApplierMeta applierMeta = new ApplierMeta();
        applierMeta.setIp("applier1");
        SourceMeta sourceMeta = new SourceMeta();
        ShardMeta shardMeta = new ShardMeta();
        ClusterMeta clusterMeta = new ClusterMeta();

        shardMeta.setDbId(shardDbId);
        shardMeta.setId(shardId);
        clusterMeta.setDbId(clusterDbId);
        clusterMeta.setId(clusterId);
        shardMeta.addApplier(applierMeta);
        sourceMeta.addShard(shardMeta);
        clusterMeta.addSource(sourceMeta);

        return clusterMeta;
    }
}