package com.ctrip.xpipe.redis.console.service;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.ClusterTblDao;
import com.ctrip.xpipe.redis.console.model.ClusterTblEntity;

/**
 * @author shyin
 *
 * Sep 26, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class ClusterServiceTest extends AbstractConsoleTest {
	@Mock
	private ClusterTblDao mockedClusterTblDao;
	@InjectMocks
	private ClusterService clusterService;
	
	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
		
		try {
			generateClusterMockData();
		} catch (Exception e) {
			logger.error("Generate Dc mock data failed.", e);
		}
	}
	
	@Test 
	public void testLoad() {
		ClusterTbl target_result = new ClusterTbl().setId(1).setClusterName("cluster1").setClusterLastModifiedTime("1234567");
		
		assertEquals(clusterService.load("cluster1").getId(), target_result.getId());
		assertEquals(clusterService.load("cluster1").getClusterName(), target_result.getClusterName());
	}
	
	private void generateClusterMockData() throws DalException {
		when(mockedClusterTblDao.findClusterByClusterName("cluster1", ClusterTblEntity.READSET_FULL)).thenReturn(
				new ClusterTbl().setId(1).setClusterName("cluster1").setClusterLastModifiedTime("1234567"));
	}
}
