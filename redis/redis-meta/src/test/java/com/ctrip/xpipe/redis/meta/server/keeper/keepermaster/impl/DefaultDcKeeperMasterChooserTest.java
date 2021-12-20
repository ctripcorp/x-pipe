package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.when;

/**
 * @author wenchao.meng
 *
 * Dec 8, 2016
 */
public class DefaultDcKeeperMasterChooserTest extends AbstractDcKeeperMasterChooserTest{

	private DefaultDcKeeperMasterChooser defaultDcKeeperMasterChooser; 
	
	@Before
	public void beforeDefaultDcKeeperMasterChooserTest() throws Exception{
		
		defaultDcKeeperMasterChooser = new DefaultDcKeeperMasterChooser(clusterDbId, shardDbId,
				multiDcService, dcMetaCache, currentMetaManager, scheduled, 
				getXpipeNettyClientKeyedObjectPool());
	}
	
	@Test
	public void testPrimaryAlgorithm(){
		
		Assert.assertNull(defaultDcKeeperMasterChooser.getKeeperMasterChooserAlgorithm());
		
		when(dcMetaCache.isCurrentDcPrimary(clusterDbId, shardDbId)).thenReturn(true);
		
		defaultDcKeeperMasterChooser.chooseKeeperMaster();

		Assert.assertTrue(defaultDcKeeperMasterChooser.getKeeperMasterChooserAlgorithm() instanceof PrimaryDcKeeperMasterChooserAlgorithm);
		
		when(dcMetaCache.isCurrentDcPrimary(clusterDbId, shardDbId)).thenReturn(false);
		
		defaultDcKeeperMasterChooser.chooseKeeperMaster();

		Assert.assertTrue(defaultDcKeeperMasterChooser.getKeeperMasterChooserAlgorithm() instanceof BackupDcKeeperMasterChooserAlgorithm);
		
	}
	
}
