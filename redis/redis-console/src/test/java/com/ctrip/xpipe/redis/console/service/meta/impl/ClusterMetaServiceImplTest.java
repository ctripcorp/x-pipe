package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.cluster.DcGroupType;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.ReplDirectionService;
import com.ctrip.xpipe.redis.console.service.ZoneService;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.SourceMeta;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
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

	@Test
	public void testGenerateBasicClusterMeta() {

		ClusterMeta clusterMeta = new ClusterMeta();
		String dcName = "jq";
		String clusterName = "cluster1";
		DcTbl dcInfo = new DcTbl();
		ClusterTbl clusterInfo = new ClusterTbl();
		clusterInfo.setClusterName(clusterName);
		clusterInfo.setClusterType("ONE_WAY");
		clusterInfo.setStatus(ClusterStatus.Normal.name());

		DcClusterTbl dcClusterInfo = new DcClusterTbl();
		List<DcTbl> clusterRelatedDc = new ArrayList<>();
		DcTbl relatedDc = new DcTbl();
		relatedDc.setDcName("oy");
		clusterRelatedDc.add(relatedDc);

		Map<Long, DcClusterTbl> dcClusterMap = new HashMap<>();

		clusterMetaServiceImpl.generateBasicClusterMeta(clusterMeta, dcName, clusterName, dcInfo, clusterInfo, dcClusterInfo,
				clusterRelatedDc, dcClusterMap);

		Assert.assertEquals(clusterName, clusterMeta.getId());
	}

	@Test
	public void testBuildSourceMeta() {
		ZoneService zoneService = mock(ZoneService.class);
		when(zoneService.findById(anyLong())).thenReturn(null);
		clusterMetaServiceImpl.setZoneService(zoneService);
		SourceMeta sourceMeta = clusterMetaServiceImpl.buildSourceMeta(new ClusterMeta(), 0L, 0L, new ArrayList<>());

		Assert.assertEquals("", sourceMeta.getSrcDc());
		Assert.assertEquals("", sourceMeta.getUpstreamDc());
		Assert.assertEquals("", sourceMeta.getRegion());
	}

	@Test
	public void testHeteroMeta() {
		ClusterMeta clusterMeta = new ClusterMeta();
		clusterMeta.setDbId(1L);
		long dcId = 1L;
		DcClusterTbl dcClusterInfo = new DcClusterTbl();
		dcClusterInfo.setGroupType(DcGroupType.MASTER.name());
		List<DcTbl> dcList = new ArrayList<>();
		Map<Long, DcClusterTbl> dcClusterMap = new HashMap<>();
		List<ShardTbl> shards = new ArrayList<>();
		ClusterTbl clusterInfo = new ClusterTbl();
		Map<Long, Long> keeperContainerId2DcMap = new HashMap<>();

		ReplDirectionService replDirectionService = mock(ReplDirectionService.class);
		List<ReplDirectionTbl> replDirectionTblList = new ArrayList<>();
		ReplDirectionTbl replDirectionTbl = new ReplDirectionTbl();
		replDirectionTbl.setClusterId(1L);
		replDirectionTbl.setToDcId(1L);
		replDirectionTblList.add(replDirectionTbl);
		when(replDirectionService.findAllReplDirectionTblsByClusterWithSrcDcAndFromDc(anyLong())).thenReturn(replDirectionTblList);
		clusterMetaServiceImpl.setReplDirectionService(replDirectionService);

		ZoneService zoneService = mock(ZoneService.class);
		when(zoneService.findById(anyLong())).thenReturn(null);
		clusterMetaServiceImpl.setZoneService(zoneService);

		clusterMetaServiceImpl.generateHeteroMeta(clusterMeta, dcId, dcClusterInfo, dcList, dcClusterMap, shards,
				clusterInfo, keeperContainerId2DcMap);

		Assert.assertEquals(1, clusterMeta.getSources().size());
	}
}
