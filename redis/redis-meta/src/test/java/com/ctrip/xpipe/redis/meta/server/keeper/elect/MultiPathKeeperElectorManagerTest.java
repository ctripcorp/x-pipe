package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaClone;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

/**
 * @author lishanglin
 * date 2021/12/4
 */
@RunWith(MockitoJUnitRunner.class)
public class MultiPathKeeperElectorManagerTest extends AbstractKeeperElectorManagerTest {

    @Mock
    private CurrentMetaManager currentMetaManager;

    private DefaultKeeperElectorManager keeperElectorManager;
    private ClusterMeta clusterMeta;
    private ShardMeta shardMeta;

    private AtomicReference<List<KeeperMeta>> surviveKeepers = new AtomicReference<>();
    private AtomicReference<KeeperMeta> activeKeeper = new AtomicReference<>();

    private KeeperMeta keeper1;
    private KeeperMeta keeper2;

    private String clusterId;
    private String shardId;
    private Long clusterDbId;
    private Long shardDbId;

    @Before
    public void beforeDefaultKeeperElectorManagerTest() throws Exception{
        keeper1 = new KeeperMeta().setId("1");
        keeper2 = new KeeperMeta().setId("2");

        keeperElectorManager = getBean(DefaultKeeperElectorManager.class);
        keeperElectorManager.initialize();

        keeperElectorManager.setCurrentMetaManager(currentMetaManager);
        keeperElectorManager.setKeeperActiveElectAlgorithmManager(new DefaultKeeperActiveElectAlgorithmManager());

        clusterMeta = differentCluster(getDc());
        shardMeta = (ShardMeta) clusterMeta.getShards().values().toArray()[0];

        doAnswer(inv -> {
            surviveKeepers.set(inv.getArgument(2, List.class));
            activeKeeper.set(inv.getArgument(3, KeeperMeta.class));
            return null;
        }).when(currentMetaManager).setSurviveKeepers(anyLong(), anyLong(), anyList(), any(KeeperMeta.class));
        when(currentMetaManager.watchKeeperIfNotWatched(anyLong(), anyLong())).thenReturn(true);

        clusterId = clusterMeta.getId();
        shardId = shardMeta.getId();
        clusterDbId = clusterMeta.getDbId();
        shardDbId = shardMeta.getDbId();
    }

    @Test
    public void testLeaderSelect() throws Exception {
        keeperElectorManager.observerShardLeader(clusterDbId, shardDbId);

        LeaderElector active = addKeeperZkNode(shardDbId, getZkClient(), keeper1);
        LeaderElector backup = addKeeperZkNode(shardDbId, getZkClient(), keeper2);
        waitConditionUntilTimeOut(()->assertSuccess(()->{
            verify(currentMetaManager, times(2)).setSurviveKeepers(anyLong(), anyLong(), anyList(), any(KeeperMeta.class));
        }));

        Assert.assertEquals(Arrays.asList(MetaClone.clone(keeper1).setActive(true), MetaClone.clone(keeper2).setActive(false)), surviveKeepers.get());
        Assert.assertEquals("1", activeKeeper.get().getId());
    }

    @Test
    public void testPathSwitch() throws Exception {
        keeperElectorManager.observerShardLeader(clusterDbId, shardDbId);

        LeaderElector active = addKeeperZkNode(clusterDbId, shardDbId, getZkClient(), keeper1);
        LeaderElector backup = addKeeperZkNode(clusterDbId, shardDbId, getZkClient(), keeper2);
        waitConditionUntilTimeOut(()->assertSuccess(()->{
            verify(currentMetaManager, times(2)).setSurviveKeepers(anyLong(), anyLong(), anyList(), any(KeeperMeta.class));
        }));

        // active switch to path for ids
        active.stop();
        waitConditionUntilTimeOut(()->assertSuccess(()->{
            verify(currentMetaManager, times(3)).setSurviveKeepers(anyLong(), anyLong(), anyList(), any(KeeperMeta.class));
        }));
        Assert.assertEquals(Arrays.asList(MetaClone.clone(keeper2).setActive(true)), surviveKeepers.get());
        Assert.assertEquals("2", activeKeeper.get().getId());

        active = addKeeperZkNode(shardDbId, getZkClient(), keeper1);
        waitConditionUntilTimeOut(()->assertSuccess(()->{
            verify(currentMetaManager, times(4)).setSurviveKeepers(anyLong(), anyLong(), anyList(), any(KeeperMeta.class));
        }));
        Assert.assertEquals(Arrays.asList(MetaClone.clone(keeper2).setActive(true), MetaClone.clone(keeper1).setActive(false)), surviveKeepers.get());
        Assert.assertEquals("2", activeKeeper.get().getId());

        // backup switch to path for ids
        backup.stop();
        backup = addKeeperZkNode(shardDbId, getZkClient(), keeper2);
        waitConditionUntilTimeOut(()->assertSuccess(()->{
            verify(currentMetaManager, times(6)).setSurviveKeepers(anyLong(), anyLong(), anyList(), any(KeeperMeta.class));
        }));
        Assert.assertEquals(Arrays.asList(MetaClone.clone(keeper1).setActive(true), MetaClone.clone(keeper2).setActive(false)), surviveKeepers.get());
        Assert.assertEquals("1", activeKeeper.get().getId());
    }

}
