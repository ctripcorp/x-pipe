package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.DefaultSlotManager;
import com.ctrip.xpipe.redis.meta.server.meta.impl.DefaultCurrentMetaManager;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import java.util.Arrays;
import java.util.HashSet;

import static org.mockito.Mockito.when;

/**
 * @author wenchao.meng
 *
 * Dec 8, 2016
 */
public class DefaultDcKeeperMasterChooserTest extends AbstractDcKeeperMasterChooserTest{

	private DefaultDcKeeperMasterChooser defaultDcKeeperMasterChooser;

	@InjectMocks
	private DefaultCurrentMetaManager metaManager;

	@Mock
	private DefaultSlotManager slotManager;

	@Before
	public void beforeDefaultDcKeeperMasterChooserTest() throws Exception{
		
		defaultDcKeeperMasterChooser = new DefaultDcKeeperMasterChooser(clusterDbId, shardDbId,
				multiDcService, dcMetaCache, currentMetaManager, scheduled, 
				getXpipeNettyClientKeyedObjectPool());
	}
	
	@Test
	public void testMasterChooserAlgorithm(){
		
		Assert.assertNull(defaultDcKeeperMasterChooser.getKeeperMasterChooserAlgorithm());

		when(dcMetaCache.isCurrentShardParentCluster(clusterDbId, shardDbId)).thenReturn(true);
		when(dcMetaCache.isCurrentDcBackUp(clusterDbId)).thenReturn(false);

		defaultDcKeeperMasterChooser.chooseKeeperMaster();

		Assert.assertTrue(defaultDcKeeperMasterChooser.getKeeperMasterChooserAlgorithm() instanceof PrimaryDcKeeperMasterChooserAlgorithm);
		
		when(dcMetaCache.isCurrentDcBackUp(clusterDbId)).thenReturn(true);

		defaultDcKeeperMasterChooser.chooseKeeperMaster();

		Assert.assertTrue(defaultDcKeeperMasterChooser.getKeeperMasterChooserAlgorithm() instanceof BackupDcKeeperMasterChooserAlgorithm);

		when(dcMetaCache.isCurrentShardParentCluster(clusterDbId, shardDbId)).thenReturn(false);

		defaultDcKeeperMasterChooser.chooseKeeperMaster();

		Assert.assertTrue(defaultDcKeeperMasterChooser.getKeeperMasterChooserAlgorithm() instanceof HeteroDownStreamDcKeeperMasterChooserAlgorithm);
	}

	@Test
	public void testWork() throws Exception {

		ClusterMeta clusterMeta = new ClusterMeta(clusterId)
				.setDbId(clusterDbId)
				.setType("ONE_WAY");
		clusterMeta.addShard(new ShardMeta(shardId)
				.setDbId(shardDbId));
		when(dcMetaCache.getClusters()).thenReturn(new HashSet<>(Arrays.asList(clusterMeta)));
		when(dcMetaCache.getClusterMeta(clusterDbId)).thenReturn(clusterMeta);
		metaManager.setDcMetaCache(dcMetaCache);
		when(slotManager.getSlotIdByKey(clusterDbId)).thenReturn(1);
		metaManager.setSlotManager(slotManager);
		metaManager.addSlot(1);

		defaultDcKeeperMasterChooser = new DefaultDcKeeperMasterChooser(clusterDbId, shardDbId,
				multiDcService, dcMetaCache, metaManager, scheduled,
				getXpipeNettyClientKeyedObjectPool());

		Pair<String, Integer> oldMaster = new Pair<>("127.0.0.1", 8080);
		try {
			metaManager.setKeeperMaster(clusterDbId, shardDbId, oldMaster.getKey(), oldMaster.getValue());
		} catch (NullPointerException e) {
			// stateHandlers 为 null
		}

		Pair<String, Integer> newMaster = metaManager.getKeeperMaster(clusterDbId, shardDbId);
		when(dcMetaCache.getPrimaryDc(clusterDbId, shardDbId))
				.thenReturn("ptjq")
				.thenReturn("ptjq")
				.thenReturn("ptoy");
		when(dcMetaCache.isCurrentShardParentCluster(clusterDbId, shardDbId)).thenReturn(true);
		when(dcMetaCache.isCurrentDcBackUp(clusterDbId)).thenReturn(true);

		when(multiDcService.getActiveKeeper("ptjq", clusterDbId, shardDbId))
				.thenReturn(new KeeperMeta().setIp("10.1.12.2").setPort(8081));

		defaultDcKeeperMasterChooser.work();

		newMaster = metaManager.getKeeperMaster(clusterDbId, shardDbId);

		Assert.assertEquals(oldMaster, newMaster);

		when(dcMetaCache.getPrimaryDc(clusterDbId, shardDbId))
				.thenReturn("ptjq")
				.thenReturn("ptjq")
				.thenReturn("ptjq");

		try {
			defaultDcKeeperMasterChooser.work();
		} catch (NullPointerException exception) {
			// stateHandlers 为 null
		}

		newMaster = metaManager.getKeeperMaster(clusterDbId, shardDbId);

		Assert.assertNotEquals(oldMaster, newMaster);


	}
	
}
