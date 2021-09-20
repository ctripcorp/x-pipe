package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.*;


/**
 * @author wenchao.meng
 *
 *         Nov 4, 2016
 */
public class BackupDcKeeperMasterChooserAlgorithmTest extends AbstractDcKeeperMasterChooserTest {

	private BackupDcKeeperMasterChooserAlgorithm backupAlgorithm;
	
	@Mock
	protected MultiDcService multiDcService;
	
	@Before
	public void beforeBackupDcKeeperMasterChooserTest() {

		backupAlgorithm = new BackupDcKeeperMasterChooserAlgorithm(clusterId, shardId, 
				dcMetaCache, currentMetaManager, multiDcService, scheduled); 
		
		when(dcMetaCache.getPrimaryDc(clusterId, shardId)).thenReturn(primaryDc);
	}

	@Test
	public void testGetUpstream() throws Exception {

		backupAlgorithm.choose();
		
		verify(multiDcService, atLeast(1)).getActiveKeeper(primaryDc, clusterId, shardId);
		

		logger.info("[testGetUpstream][getActiveKeeper give a result]");
		KeeperMeta keeperMeta = new KeeperMeta();
		keeperMeta.setIp("localhost");
		keeperMeta.setPort(randomPort());
		when(multiDcService.getActiveKeeper(primaryDc, clusterId, shardId)).thenReturn(keeperMeta);
		
		Assert.assertEquals(new Pair<>(keeperMeta.getIp(), keeperMeta.getPort()), backupAlgorithm.choose());
				
		verify(multiDcService, atLeast(1)).getActiveKeeper(primaryDc, clusterId, shardId);
	}

	@Test
	public void testVerify(){
		
		currentMetaManager.getClusterMeta(clusterId);
		currentMetaManager.getClusterMeta(clusterId);
		currentMetaManager.getClusterMeta(clusterId + "1");
		verify(currentMetaManager, times(2)).getClusterMeta(clusterId);
		verify(currentMetaManager, times(1)).getClusterMeta(clusterId + "1");
		
	}

}
