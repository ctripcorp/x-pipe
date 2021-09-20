package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.cluster.DefaultLeaderElector;
import com.ctrip.xpipe.cluster.ElectContext;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerContextTest;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperActiveElectAlgorithm;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperActiveElectAlgorithmManager;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.zk.ZkClient;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

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
public class DefaultKeeperElectorManagerTest extends AbstractMetaServerContextTest{

	@Mock
	private CurrentMetaManager currentMetaManager;
	
	@Mock
	private KeeperActiveElectAlgorithmManager KeeperActiveElectAlgorithmManager;
	
	@Mock 
	private KeeperActiveElectAlgorithm keeperActiveElectAlgorithm;
	
	private DefaultKeeperElectorManager keeperElectorManager;
	private ClusterMeta clusterMeta;
	private ShardMeta shardMeta;

	private int sleepForZkMilli = 300;
	
	@Before
	public void beforeDefaultKeeperElectorManagerTest() throws Exception{
		
		when(KeeperActiveElectAlgorithmManager.get(anyString(), anyString())).thenReturn(keeperActiveElectAlgorithm);
		when(keeperActiveElectAlgorithm.select(anyString(), anyString(), anyList())).thenReturn(new KeeperMeta());

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

		keeperElectorManager.updateShardLeader(prefix, dataList, clusterMeta.getId(), shardMeta.getId());

		verify(keeperActiveElectAlgorithm).select(eq(clusterMeta.getId()), eq(shardMeta.getId()), argThat(new ArgumentMatcher<List<KeeperMeta>>() {

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

		when(currentMetaManager.watchIfNotWatched(anyString(), anyString())).thenReturn(true);
		keeperElectorManager.observerShardLeader(clusterId, shardId);
		addKeeperZkNode(clusterId, shardId, getZkClient());
		sleep(sleepForZkMilli);
		verify(currentMetaManager).setSurviveKeepers(anyString(), anyString(), anyList(), any(KeeperMeta.class));
		verify(currentMetaManager).addResource(anyString(), anyString(), any(Releasable.class));

		when(currentMetaManager.watchIfNotWatched(anyString(), anyString())).thenReturn(false);
		keeperElectorManager.observerShardLeader(clusterId, shardId);
		sleep(sleepForZkMilli);
		verify(currentMetaManager).setSurviveKeepers(anyString(), anyString(), anyList(), any(KeeperMeta.class));
		verify(currentMetaManager).addResource(anyString(), anyString(), any(Releasable.class));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testAddWatch() throws Exception{

		when(currentMetaManager.watchIfNotWatched(anyString(), anyString())).thenReturn(true);
		
		keeperElectorManager.update(new NodeAdded<>(clusterMeta), null);
		//change notify
		addKeeperZkNode(clusterMeta.getId(), shardMeta.getId(), getZkClient());
		
		sleep(sleepForZkMilli);
		verify(keeperActiveElectAlgorithm, times(1)).select(eq(clusterMeta.getId()), eq(shardMeta.getId()), anyList());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRemoveWatch() throws Exception{

		when(currentMetaManager.watchIfNotWatched(anyString(), anyString())).thenReturn(true);
		
		final AtomicReference<Releasable>  release = new AtomicReference<Releasable>(null);
		doAnswer(new Answer<Void>() {

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				release.set((Releasable) invocation.getArguments()[2]);
				return null;
			}
		}).when(currentMetaManager).addResource(anyString(), anyString(), any(Releasable.class));
		
		keeperElectorManager.update(new NodeAdded<>(clusterMeta), null);

		addKeeperZkNode(clusterMeta.getId(), shardMeta.getId(), getZkClient());
		sleep(sleepForZkMilli);
		verify(keeperActiveElectAlgorithm).select(eq(clusterMeta.getId()), eq(shardMeta.getId()), anyList());

		release.get().release();

		addKeeperZkNode(clusterMeta.getId(), shardMeta.getId(), getZkClient());
		sleep(sleepForZkMilli);
		verify(keeperActiveElectAlgorithm, times(1)).select(eq(clusterMeta.getId()), eq(shardMeta.getId()), anyList());
	}


	private void addKeeperZkNode(String clusterId, String shardId, ZkClient zkClient) throws Exception {

		addKeeperZkNode(clusterId, shardId, zkClient, 0);

	}

	private void addKeeperZkNode(String clusterId, String shardId, ZkClient zkClient, int idLen) throws Exception {
		
		String leaderElectionZKPath = MetaZkConfig.getKeeperLeaderLatchPath(clusterId, shardId);
		String leaderElectionID;
		if(idLen == 0){
			leaderElectionID = MetaZkConfig.getKeeperLeaderElectionId(new KeeperMeta());
		}else{
			leaderElectionID = MetaZkConfig.getKeeperLeaderElectionId(new KeeperMeta().setId(randomString(idLen)));
		}

		ElectContext ctx = new ElectContext(leaderElectionZKPath, leaderElectionID);
		LeaderElector leaderElector = new DefaultLeaderElector(ctx, zkClient.get());
		leaderElector.elect();
		
	}


}
