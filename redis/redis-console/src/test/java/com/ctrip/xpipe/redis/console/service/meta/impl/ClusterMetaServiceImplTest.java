package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.when;


/**
 * @author wenchao.meng
 *
 * Mar 17, 2017
 */
public class ClusterMetaServiceImplTest extends AbstractConsoleIntegrationTest{

	@Autowired
	private ClusterMetaServiceImpl  clusterMetaServiceImpl;
	
	@Mock
	private MigrationService migrationService;


	
	@Before
	public void beforeClusterMetaServiceImplTest(){
		MockitoAnnotations.initMocks(this);
//		clusterMetaServiceImpl = new ClusterMetaServiceImpl();
		clusterMetaServiceImpl.setMigrationService(migrationService);
	}
	
	
	@Test
	public void testGetClusterMetaCurrentPrimaryDcMigrating(){
		
		long currentActiveDcId = randomInt();
		long clusterId = randomInt();
		long destinationDcId = currentActiveDcId + 1;
		long migrationEventId = randomInt();
		
		DcTbl dcTbl = new DcTbl();
		
		ClusterTbl clusterTbl = new ClusterTbl();
		clusterTbl.setId(clusterId);
		clusterTbl.setActivedcId(currentActiveDcId);
		clusterTbl.setStatus(ClusterStatus.Migrating.toString());
		clusterTbl.setMigrationEventId(migrationEventId);
		
		
		when(migrationService.findMigrationCluster(migrationEventId, clusterId)).thenReturn(new MigrationClusterTbl().setDestinationDcId(destinationDcId));
		
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
		
		
		when(migrationService.findMigrationCluster(anyLong(), anyLong())).thenReturn(new MigrationClusterTbl().setDestinationDcId(destinationDcId));

		dcTbl.setId(destinationDcId);
		for(ClusterStatus clusterStatus : ClusterStatus.values()){
			if(clusterStatus == ClusterStatus.Migrating){
				continue;
			}
			clusterTbl.setStatus(clusterStatus.toString());
			Assert.assertEquals(currentActiveDcId, clusterMetaServiceImpl.getClusterMetaCurrentPrimaryDc(dcTbl, clusterTbl));
		}
	}

	@Test
	public void testCurrentPrimaryDcPublish() {
		long currentActiveDcId = 1;
		long clusterId = randomInt();
		long destinationDcId = 2;
		long migrationEventId = randomInt();

		DcTbl dcTbl = new DcTbl();
		dcTbl.setId(destinationDcId);

		ClusterTbl clusterTbl = new ClusterTbl();
		clusterTbl.setId(clusterId);
		clusterTbl.setActivedcId(currentActiveDcId);
		clusterTbl.setStatus(ClusterStatus.Migrating.name());
		clusterTbl.setMigrationEventId(migrationEventId);


		when(migrationService.findMigrationCluster(migrationEventId, clusterId)).thenReturn(new MigrationClusterTbl().setDestinationDcId(destinationDcId));
		long target = clusterMetaServiceImpl.getClusterMetaCurrentPrimaryDc(dcTbl, clusterTbl);
		Assert.assertEquals(destinationDcId, target);
	}

}
