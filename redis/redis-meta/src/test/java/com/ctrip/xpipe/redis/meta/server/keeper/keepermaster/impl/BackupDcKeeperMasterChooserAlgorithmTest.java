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

		backupAlgorithm = new BackupDcKeeperMasterChooserAlgorithm(clusterDbId, shardDbId,
				dcMetaCache, currentMetaManager, multiDcService, scheduled); 
		
		when(dcMetaCache.getPrimaryDc(clusterDbId, shardDbId)).thenReturn(primaryDc);
		when(dcMetaCache.isCurrentDcPrimary(clusterDbId, shardDbId)).thenReturn(false);
		
	}

	@Test
	public void testGetUpstream() throws Exception {

		backupAlgorithm.choose();
		
		verify(multiDcService, atLeast(1)).getActiveKeeper(primaryDc, clusterDbId, shardDbId);
		

		logger.info("[testGetUpstream][getActiveKeeper give a result]");
		KeeperMeta keeperMeta = new KeeperMeta();
		keeperMeta.setIp("localhost");
		keeperMeta.setPort(randomPort());
		when(multiDcService.getActiveKeeper(primaryDc, clusterDbId, shardDbId)).thenReturn(keeperMeta);
		
		Assert.assertEquals(new Pair<>(keeperMeta.getIp(), keeperMeta.getPort()), backupAlgorithm.choose());
				
		verify(multiDcService, atLeast(1)).getActiveKeeper(primaryDc, clusterDbId, shardDbId);
	}

	@Test
	public void testVerify(){
		
		currentMetaManager.getClusterMeta(clusterDbId);
		currentMetaManager.getClusterMeta(clusterDbId);
		currentMetaManager.getClusterMeta(clusterDbId + 1);
		verify(currentMetaManager, times(2)).getClusterMeta(clusterDbId);
		verify(currentMetaManager, times(1)).getClusterMeta(clusterDbId + 1);
		
	}

}
