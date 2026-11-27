package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.HealthState;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperActiveElectAlgorithm;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperActiveElectAlgorithmManager;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.nio.charset.StandardCharsets;
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
	private DcMetaCache dcMetaCache;

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
		keeperElectorManager.setDcMetaCache(dcMetaCache);
		when(dcMetaCache.getShardKeepers(anyLong(), anyLong())).thenReturn(Collections.emptyList());

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
	public void testUpdateShardLeaderEmptyChildrenClearsSurvive() {
		when(keeperActiveElectAlgorithm.select(anyLong(), anyLong(), anyList())).thenReturn(null);

		keeperElectorManager.updateShardLeader(
				Collections.singletonList(Collections.emptyList()), clusterMeta.getDbId(), shardMeta.getDbId());

		verify(currentMetaManager).setSurviveKeepers(eq(clusterMeta.getDbId()), eq(shardMeta.getDbId()),
				argThat(keepers -> keepers != null && keepers.isEmpty()), isNull());
	}

	@Test
	public void testUpdateShardLeaderWritesSurviveWhenSelectReturnsNull() {
		String prefix = "/path";
		List<ChildData> dataList = new LinkedList<>();
		dataList.add(new ChildData(prefix + "/" + randomString(10) + "-latch-01", null,
				JsonCodec.INSTANCE.encodeAsBytes(new KeeperMeta().setId("127.0.0.1").setPort(4000))));
		when(keeperActiveElectAlgorithm.select(anyLong(), anyLong(), anyList())).thenReturn(null);

		keeperElectorManager.updateShardLeader(Collections.singletonList(dataList), clusterMeta.getDbId(), shardMeta.getDbId());

		verify(currentMetaManager).setSurviveKeepers(eq(clusterMeta.getDbId()), eq(shardMeta.getDbId()),
				argThat(keepers -> keepers != null && keepers.size() == 1), isNull());
	}

	@Test
	public void testObserverShardLeader() throws Exception {
		Long clusterDbId = clusterMeta.getDbId();
		Long shardDbId = shardMeta.getDbId();

		when(currentMetaManager.watchKeeperIfNotWatched(anyLong(), anyLong())).thenReturn(true);
		keeperElectorManager.observerShardLeader(clusterDbId, shardDbId);
		addKeeperZkNode(clusterDbId, shardDbId, getZkClient());
		waitConditionUntilTimeOut(()->assertSuccess(()->{
			verify(currentMetaManager).setSurviveKeepers(anyLong(), anyLong(), anyList(), any(KeeperMeta.class));
			verify(currentMetaManager).addResource(anyLong(), anyLong(), any(Releasable.class));
		}));

		when(currentMetaManager.watchKeeperIfNotWatched(anyLong(), anyLong())).thenReturn(false);
		keeperElectorManager.observerShardLeader(clusterDbId, shardDbId);
		waitConditionUntilTimeOut(()->assertSuccess(()->{
			verify(currentMetaManager).setSurviveKeepers(anyLong(), anyLong(), anyList(), any(KeeperMeta.class));
			verify(currentMetaManager).addResource(anyLong(), anyLong(), any(Releasable.class));
		}));
	}

	@Test
	public void testKeeperDataChangeTriggersReElectionWithoutRecreatingNode() throws Exception {
		Long clusterDbId = clusterMeta.getDbId();
		Long shardDbId = shardMeta.getDbId();
		KeeperMeta first = new KeeperMeta().setId("keeper-1").setIp("127.0.0.1").setPort(4101);
		KeeperMeta second = new KeeperMeta().setId("keeper-2").setIp("127.0.0.1").setPort(4102);
		AtomicReference<KeeperMeta> selected = new AtomicReference<>();
		AtomicReference<List<KeeperMeta>> survivors = new AtomicReference<>();

		when(keeperActiveElectAlgorithm.select(eq(clusterDbId), eq(shardDbId), anyList()))
				.thenAnswer(invocation -> {
					List<KeeperMeta> candidates = invocation.getArgument(2);
					for (KeeperMeta candidate : candidates) {
						if (!HealthState.DOWN.name().equals(candidate.getHealthState())) {
							return candidate;
						}
					}
					return candidates.isEmpty() ? null : candidates.get(0);
				});
		doAnswer(invocation -> {
			survivors.set(invocation.getArgument(2));
			selected.set(invocation.getArgument(3));
			return true;
		}).when(currentMetaManager).setSurviveKeepers(eq(clusterDbId), eq(shardDbId), anyList(), any());
		when(currentMetaManager.watchKeeperIfNotWatched(clusterDbId, shardDbId)).thenReturn(true);

		keeperElectorManager.observerShardLeader(clusterDbId, shardDbId);
		LeaderElector firstElector = addKeeperZkNode(clusterDbId, shardDbId, getZkClient(), first);
		LeaderElector secondElector = addKeeperZkNode(clusterDbId, shardDbId, getZkClient(), second);
		try {
			waitConditionUntilTimeOut(() -> selected.get() != null
					&& "keeper-1".equals(selected.get().getId())
					&& survivors.get() != null && survivors.get().size() == 2);

			String parentPath = MetaZkConfig.getKeeperLeaderLatchPath(clusterDbId, shardDbId);
			List<String> childrenBefore = getZkClient().get().getChildren().forPath(parentPath);
			String firstPath = null;
			for (String child : childrenBefore) {
				String childPath = parentPath + "/" + child;
				KeeperMeta data = JsonCodec.INSTANCE.decode(
						new String(getZkClient().get().getData().forPath(childPath), StandardCharsets.UTF_8),
						KeeperMeta.class);
				if ("keeper-1".equals(data.getId())) {
					firstPath = childPath;
					break;
				}
			}
			org.junit.Assert.assertNotNull(firstPath);

			first.setHealthState(HealthState.DOWN.name());
			getZkClient().get().setData().forPath(firstPath, JsonCodec.INSTANCE.encodeAsBytes(first));

			waitConditionUntilTimeOut(() -> selected.get() != null
					&& "keeper-2".equals(selected.get().getId()));
			org.junit.Assert.assertEquals(childrenBefore,
					getZkClient().get().getChildren().forPath(parentPath));
			org.junit.Assert.assertTrue(survivors.get().stream().anyMatch(keeper ->
					"keeper-1".equals(keeper.getId())
							&& HealthState.DOWN.name().equals(keeper.getHealthState())));
		} finally {
			firstElector.stop();
			secondElector.stop();
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testAddWatch() throws Exception{

		when(currentMetaManager.watchKeeperIfNotWatched(anyLong(), anyLong())).thenReturn(true);

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

		when(currentMetaManager.watchKeeperIfNotWatched(anyLong(), anyLong())).thenReturn(true);
		
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
