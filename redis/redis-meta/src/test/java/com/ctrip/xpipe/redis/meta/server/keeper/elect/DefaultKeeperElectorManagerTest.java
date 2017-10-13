package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;

import static org.mockito.Mockito.*;

import java.util.concurrent.atomic.AtomicReference;

import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.cluster.DefaultLeaderElector;
import com.ctrip.xpipe.cluster.ElectContext;
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
	
	@Before
	public void beforeDefaultKeeperElectorManagerTest() throws Exception{
		
		when(KeeperActiveElectAlgorithmManager.get(anyString(), anyString())).thenReturn(keeperActiveElectAlgorithm);

		keeperElectorManager = getBean(DefaultKeeperElectorManager.class);
		keeperElectorManager.initialize();
		
		keeperElectorManager.setCurrentMetaManager(currentMetaManager);
		keeperElectorManager.setKeeperActiveElectAlgorithmManager(KeeperActiveElectAlgorithmManager);

		clusterMeta = differentCluster(getDc());
		shardMeta = (ShardMeta) clusterMeta.getShards().values().toArray()[0];
	}

	@Test
	public void testObserverShardLeader(){

		when(currentMetaManager.watchIfNotWatched(anyString(), anyString())).thenReturn(true);
		keeperElectorManager.observerShardLeader(clusterMeta.getId(), shardMeta.getId());
		verify(currentMetaManager).setSurviveKeepers(anyString(), anyString(), anyList(), any(KeeperMeta.class));
		verify(currentMetaManager).addResource(anyString(), anyString(), any(Releasable.class));

		when(currentMetaManager.watchIfNotWatched(anyString(), anyString())).thenReturn(false);
		keeperElectorManager.observerShardLeader(clusterMeta.getId(), shardMeta.getId());
		verify(currentMetaManager, times(2)).setSurviveKeepers(anyString(), anyString(), anyList(), any(KeeperMeta.class));
		verify(currentMetaManager).addResource(anyString(), anyString(), any(Releasable.class));

	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testAddWatch() throws Exception{
		
		
		when(currentMetaManager.hasShard(anyString(), anyString())).thenReturn(true);
		when(currentMetaManager.watchIfNotWatched(anyString(), anyString())).thenReturn(true);
		
		keeperElectorManager.update(new NodeAdded<>(clusterMeta), null);
		
		verify(keeperActiveElectAlgorithm).select(eq(clusterMeta.getId()), eq(shardMeta.getId()), anyList());
		
		//change notify
		addKeeperZkNode(clusterMeta.getId(), shardMeta.getId(), getZkClient());
		
		sleep(100);
		verify(keeperActiveElectAlgorithm, times(2)).select(eq(clusterMeta.getId()), eq(shardMeta.getId()), anyList());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRemoveWatch() throws Exception{

		when(currentMetaManager.hasShard(anyString(), anyString())).thenReturn(true);
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
		
		verify(keeperActiveElectAlgorithm).select(eq(clusterMeta.getId()), eq(shardMeta.getId()), anyList());

		release.get().release();

		addKeeperZkNode(clusterMeta.getId(), shardMeta.getId(), getZkClient());
		
		sleep(100);
		verify(keeperActiveElectAlgorithm, times(1)).select(eq(clusterMeta.getId()), eq(shardMeta.getId()), anyList());
	}
	
	
	private void addKeeperZkNode(String clusterId, String shardId, ZkClient zkClient) throws Exception {
		
		String leaderElectionZKPath = MetaZkConfig.getKeeperLeaderLatchPath(clusterId, shardId);
		String leaderElectionID = MetaZkConfig.getKeeperLeaderElectionId(new KeeperMeta());
		ElectContext ctx = new ElectContext(leaderElectionZKPath, leaderElectionID);
		LeaderElector leaderElector = new DefaultLeaderElector(ctx, zkClient.get());
		leaderElector.elect();
		
	}


}
