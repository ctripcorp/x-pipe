package com.ctrip.xpipe.redis.console.service.meta.impl;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import static org.mockito.Mockito.*;

import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;


/**
 * @author wenchao.meng
 *
 * Mar 17, 2017
 */
@RunWith(MockitoJUnitRunner.class)
public class ClusterMetaServiceImplTest extends AbstractConsoleTest{
	
	private ClusterMetaServiceImpl  clusterMetaServiceImpl;
	
	@Mock
	private MigrationService migrationService;
	
	
	
	@Before
	public void beforeClusterMetaServiceImplTest(){
		clusterMetaServiceImpl = new ClusterMetaServiceImpl();
		clusterMetaServiceImpl.setMigrationService(migrationService);
	}
	
	
	@Test
	public void testGetClusterMetaCurrentPrimaryDcMigrating(){
		
		long currentActiveDcId = randomInt();
		long clusterId = randomInt();
		long destinationDcId = currentActiveDcId + 1;
		
		DcTbl dcTbl = new DcTbl();
		
		ClusterTbl clusterTbl = new ClusterTbl();
		clusterTbl.setId(clusterId);
		clusterTbl.setActivedcId(currentActiveDcId);
		clusterTbl.setStatus(ClusterStatus.Migrating.toString());
		
		
		when(migrationService.findLatestUnfinishedMigrationCluster(clusterId)).thenReturn(new MigrationClusterTbl().setDestinationDcId(destinationDcId));
		
		dcTbl.setId(destinationDcId);
		Assert.assertEquals(destinationDcId, clusterMetaServiceImpl.getClusterMetaCurrentPrimaryDc(dcTbl, clusterTbl));

		dcTbl.setId(destinationDcId + 1);
		Assert.assertEquals(currentActiveDcId, clusterMetaServiceImpl.getClusterMetaCurrentPrimaryDc(dcTbl, clusterTbl));
		dcTbl.setId(currentActiveDcId);
		Assert.assertEquals(currentActiveDcId, clusterMetaServiceImpl.getClusterMetaCurrentPrimaryDc(dcTbl, clusterTbl));

	}

	@Test
	public void testGetClusterMetaCurrentPrimaryDcNotMigrating(){
		
		long currentActiveDcId = randomInt();
		long clusterId = randomInt();
		long destinationDcId = currentActiveDcId + 1;
		
		DcTbl dcTbl = new DcTbl();
		
		ClusterTbl clusterTbl = new ClusterTbl();
		clusterTbl.setId(clusterId);
		clusterTbl.setActivedcId(currentActiveDcId);
		
		
		when(migrationService.findLatestUnfinishedMigrationCluster(clusterId)).thenReturn(new MigrationClusterTbl().setDestinationDcId(destinationDcId));

		dcTbl.setId(destinationDcId);
		for(ClusterStatus clusterStatus : ClusterStatus.values()){
			if(clusterStatus == ClusterStatus.Migrating){
				continue;
			}
			clusterTbl.setStatus(clusterStatus.toString());
			Assert.assertEquals(currentActiveDcId, clusterMetaServiceImpl.getClusterMetaCurrentPrimaryDc(dcTbl, clusterTbl));
		}
	}

}
