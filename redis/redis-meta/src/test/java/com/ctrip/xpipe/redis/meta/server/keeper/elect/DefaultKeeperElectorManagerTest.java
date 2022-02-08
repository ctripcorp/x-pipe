package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperActiveElectAlgorithm;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperActiveElectAlgorithmManager;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
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

import static org.mockito.Mockito.*;

/**
 * @author wenchao.meng
 *
 * Sep 13, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultKeeperElectorManagerTest extends AbstractKeeperElectorManagerTest {

	@Mock
	private CurrentMetaManager currentMetaManager;
	
	@Mock
	private KeeperActiveElectAlgorithmManager KeeperActiveElectAlgorithmManager;
	
	@Mock 
	private KeeperActiveElectAlgorithm keeperActiveElectAlgorithm;
	
	private DefaultKeeperElectorManager keeperElectorManager;
	private ClusterMeta clusterMeta;
	private ShardMeta shardMeta;

	@Before
	public void beforeDefaultKeeperElectorManagerTest() throws Exception{
		
		when(KeeperActiveElectAlgorithmManager.get(anyLong(), anyLong())).thenReturn(keeperActiveElectAlgorithm);
		when(keeperActiveElectAlgorithm.select(anyLong(), anyLong(), anyList())).thenReturn(new KeeperMeta());

		keeperElectorManager = getBean(DefaultKeeperElectorManager.class);
		keeperElectorManager.initialize();
		
		keeperElectorManager.setCurrentMetaManager(currentMetaManager);
		keeperElectorManager.setKeeperActiveElectAlgorithmManager(KeeperActiveElectAlgorithmManager);

		clusterMeta = differentCluster(getDc());
		shardMeta = (ShardMeta) clusterMeta.getShards().values().toArray()[0];
	}

	@Test
	public void testUpdateShardLeader(){

		String prefix = "/path";
		List<ChildData> dataList = new LinkedList<>();
		final int portBegin = 4000;
		final int count = 3;

		dataList.add(new ChildData(prefix + "/" + randomString(10) +"-latch-02", null, JsonCodec.INSTANCE.encodeAsBytes(new KeeperMeta().setId("127.0.0.1").setPort(portBegin + 1))));
		dataList.add(new ChildData(prefix + "/"+ randomString(10) + "-latch-03", null, JsonCodec.INSTANCE.encodeAsBytes(new KeeperMeta().setId("127.0.0.1").setPort(portBegin + 2))));
		dataList.add(new ChildData(prefix + "/"+ randomString(10) + "-latch-01", null, JsonCodec.INSTANCE.encodeAsBytes(new KeeperMeta().setId("127.0.0.1").setPort(portBegin))));

		keeperElectorManager.updateShardLeader(Collections.singletonList(dataList), clusterMeta.getDbId(), shardMeta.getDbId());

		verify(keeperActiveElectAlgorithm).select(eq(clusterMeta.getDbId()), eq(shardMeta.getDbId()), argThat(new ArgumentMatcher<List<KeeperMeta>>() {

			@Override
			public boolean matches(List<KeeperMeta> item) {
				List<KeeperMeta> keepers = item;
				if(keepers.size() != count){
					return false;
				}
				KeeperMeta prefix = null;
				for(KeeperMeta keeperMeta : keepers){
					if(prefix != null){
						if(keeperMeta.getPort() < prefix.getPort()){
							return false;
						}
					}
					prefix = keeperMeta;
				}
				return true;
			}

		}));
	}

	@Test
	public void testObserverShardLeader() throws Exception {

		String clusterId = clusterMeta.getId();
		String shardId = shardMeta.getId();
		Long clusterDbId = clusterMeta.getDbId();
		Long shardDbId = shardMeta.getDbId();

		when(currentMetaManager.watchIfNotWatched(anyLong(), anyLong())).thenReturn(true);
		keeperElectorManager.observerShardLeader(clusterDbId, shardDbId);
		addKeeperZkNode(clusterDbId, shardDbId, getZkClient());
		waitConditionUntilTimeOut(()->assertSuccess(()->{
			verify(currentMetaManager).setSurviveKeepers(anyLong(), anyLong(), anyList(), any(KeeperMeta.class));
			verify(currentMetaManager).addResource(anyLong(), anyLong(), any(Releasable.class));
		}));

		when(currentMetaManager.watchIfNotWatched(anyLong(), anyLong())).thenReturn(false);
		keeperElectorManager.observerShardLeader(clusterDbId, shardDbId);
		waitConditionUntilTimeOut(()->assertSuccess(()->{
			verify(currentMetaManager).setSurviveKeepers(anyLong(), anyLong(), anyList(), any(KeeperMeta.class));
			verify(currentMetaManager).addResource(anyLong(), anyLong(), any(Releasable.class));
		}));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testAddWatch() throws Exception{

		when(currentMetaManager.watchIfNotWatched(anyLong(), anyLong())).thenReturn(true);

		keeperElectorManager.update(new NodeAdded<>(clusterMeta), null);
		//change notify
		addKeeperZkNode(clusterMeta.getDbId(), shardMeta.getDbId(), getZkClient());

		waitConditionUntilTimeOut(()->assertSuccess(()->{
			verify(keeperActiveElectAlgorithm, atLeastOnce()).select(eq(clusterMeta.getDbId()), eq(shardMeta.getDbId()), anyList());
		}));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRemoveWatch() throws Exception{

		when(currentMetaManager.watchIfNotWatched(anyLong(), anyLong())).thenReturn(true);
		
		final AtomicReference<Releasable>  release = new AtomicReference<Releasable>(null);
		doAnswer(new Answer<Void>() {

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				release.set((Releasable) invocation.getArguments()[2]);
				return null;
			}
		}).when(currentMetaManager).addResource(anyLong(), anyLong(), any(Releasable.class));
		
		keeperElectorManager.update(new NodeAdded<>(clusterMeta), null);

		addKeeperZkNode(clusterMeta.getDbId(), shardMeta.getDbId(), getZkClient());
		waitConditionUntilTimeOut(()->assertSuccess(()-> {
			verify(keeperActiveElectAlgorithm).select(eq(clusterMeta.getDbId()), eq(shardMeta.getDbId()), anyList());
		}));

		release.get().release();

		addKeeperZkNode(clusterMeta.getDbId(), shardMeta.getDbId(), getZkClient());
		waitConditionUntilTimeOut(()->assertSuccess(()-> {
			verify(keeperActiveElectAlgorithm, times(1)).select(eq(clusterMeta.getDbId()), eq(shardMeta.getDbId()), anyList());
		}));
	}

}
