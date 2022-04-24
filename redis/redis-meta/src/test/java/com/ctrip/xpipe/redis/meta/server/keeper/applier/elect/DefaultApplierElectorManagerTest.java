package com.ctrip.xpipe.redis.meta.server.keeper.applier.elect;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.meta.server.keeper.applier.ApplierActiveElectAlgorithm;
import com.ctrip.xpipe.redis.meta.server.keeper.applier.ApplierActiveElectAlgorithmManager;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * @author ayq
 * <p>
 * 2022/4/17 21:29
 */

@RunWith(MockitoJUnitRunner.class)
public class DefaultApplierElectorManagerTest extends AbstractApplierElectorManagerTest {

    @Mock
    private CurrentMetaManager currentMetaManager;

    @Mock
    private ApplierActiveElectAlgorithmManager applierActiveElectAlgorithmManager;

    @Mock
    private ApplierActiveElectAlgorithm applierActiveElectAlgorithm;

    @Mock
    private DcMetaCache dcMetaCache;

    @Mock
    private MultiDcService multiDcService;

    private DefaultApplierElectorManager applierElectorManager;
    private ClusterMeta clusterMeta;
    private ShardMeta shardMeta;

    @Before
    public void beforeDefaultApplierElectorManagerTest() throws Exception {

        when(applierActiveElectAlgorithmManager.get(anyLong(), anyLong())).thenReturn(applierActiveElectAlgorithm);
        when(applierActiveElectAlgorithm.select(anyLong(), anyLong(), anyList())).thenReturn(new ApplierMeta());

        applierElectorManager = getBean(DefaultApplierElectorManager.class);
        applierElectorManager.initialize();

        applierElectorManager.setCurrentMetaManager(currentMetaManager);
        applierElectorManager.setApplierActiveElectAlgorithmManager(applierActiveElectAlgorithmManager);
        applierElectorManager.setDcMetaCache(dcMetaCache);
        applierElectorManager.setMultiDcService(multiDcService);

        clusterMeta = differentCluster("oy", 2);
        shardMeta = (ShardMeta) clusterMeta.getAllShards().values().toArray()[0];
    }

    @Test
    public void testUpdateShardLeader() {

        String prefix = "/path";
        List<ChildData> dataList = new LinkedList<>();
        final int portBegin = 4000;
        final int count = 3;

        dataList.add(new ChildData(prefix + "/" + randomString(10) +"-latch-02", null, JsonCodec.INSTANCE.encodeAsBytes(new ApplierMeta().setId("127.0.0.1").setPort(portBegin + 1))));
        dataList.add(new ChildData(prefix + "/"+ randomString(10) + "-latch-03", null, JsonCodec.INSTANCE.encodeAsBytes(new ApplierMeta().setId("127.0.0.1").setPort(portBegin + 2))));
        dataList.add(new ChildData(prefix + "/"+ randomString(10) + "-latch-01", null, JsonCodec.INSTANCE.encodeAsBytes(new ApplierMeta().setId("127.0.0.1").setPort(portBegin))));

        when(multiDcService.getSids(any(), any(), anyLong(), anyLong())).thenReturn("a1");

        applierElectorManager.updateShardLeader(Collections.singletonList(dataList), clusterMeta.getDbId(), shardMeta.getDbId());

        verify(applierActiveElectAlgorithm).select(eq(clusterMeta.getDbId()), eq(shardMeta.getDbId()), argThat(new ArgumentMatcher<List<ApplierMeta>>() {

            @Override
            public boolean matches(List<ApplierMeta> item) {
                List<ApplierMeta> appliers = item;
                if(appliers.size() != count){
                    return false;
                }
                ApplierMeta prefix = null;
                for(ApplierMeta applierMeta : appliers){
                    if(prefix != null){
                        if(applierMeta.getPort() < prefix.getPort()){
                            return false;
                        }
                    }
                    prefix = applierMeta;
                }
                return true;
            }

        }));
    }

    @Test
    public void testObserverShardLeader() throws Exception {

        Long clusterDbId = clusterMeta.getDbId();
        Long shardDbId = shardMeta.getDbId();

        when(currentMetaManager.watchApplierIfNotWatched(anyLong(), anyLong())).thenReturn(true);
        applierElectorManager.observerShardLeader(clusterDbId, shardDbId);
        addApplierZkNode(clusterDbId, shardDbId, getZkClient());
        waitConditionUntilTimeOut(()->assertSuccess(()->{
            verify(currentMetaManager).setSurviveAppliersAndNotify(anyLong(), anyLong(), anyList(), any(ApplierMeta.class), any());
            verify(currentMetaManager).addResource(anyLong(), anyLong(), any(Releasable.class));
        }));

        when(currentMetaManager.watchApplierIfNotWatched(anyLong(), anyLong())).thenReturn(false);
        applierElectorManager.observerShardLeader(clusterDbId, shardDbId);
        waitConditionUntilTimeOut(()->assertSuccess(()->{
            verify(currentMetaManager).setSurviveAppliersAndNotify(anyLong(), anyLong(), anyList(), any(ApplierMeta.class), any());
            verify(currentMetaManager).addResource(anyLong(), anyLong(), any(Releasable.class));
        }));
    }

    @Test
    public void testAddWatch() throws Exception {

        when(currentMetaManager.watchApplierIfNotWatched(anyLong(), anyLong())).thenReturn(true);

        applierElectorManager.update(new NodeAdded<>(clusterMeta), null);
        //change notify
        addApplierZkNode(clusterMeta.getDbId(), shardMeta.getDbId(), getZkClient());

        waitConditionUntilTimeOut(()->assertSuccess(()->{
            verify(applierActiveElectAlgorithm, atLeastOnce()).select(eq(clusterMeta.getDbId()), eq(shardMeta.getDbId()), anyList());
        }));
    }

    @Test
    public void testRemoveWatch() throws Exception {

        when(currentMetaManager.watchApplierIfNotWatched(anyLong(), anyLong())).thenReturn(true);

        final AtomicReference<Releasable> release = new AtomicReference<>(null);

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                release.set((Releasable) invocation.getArguments()[2]);
                return null;
            }
        }).when(currentMetaManager).addResource(anyLong(), anyLong(), any(Releasable.class));

        applierElectorManager.update(new NodeAdded<>(clusterMeta), null);

        addApplierZkNode(clusterMeta.getDbId(), shardMeta.getDbId(), getZkClient());
        waitConditionUntilTimeOut(()->assertSuccess(()->{
            verify(applierActiveElectAlgorithm).select(eq(clusterMeta.getDbId()), eq(shardMeta.getDbId()), anyList());
        }));

        release.get().release();

        addApplierZkNode(clusterMeta.getDbId(), shardMeta.getDbId(), getZkClient());
        waitConditionUntilTimeOut(()->assertSuccess(()->{
            verify(applierActiveElectAlgorithm, times(1)).select(eq(clusterMeta.getDbId()), eq(shardMeta.getDbId()), anyList());
        }));
    }
}