package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.dao.ClusterDao;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.service.impl.ClusterServiceImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * @author shyin
 *
 *         Sep 26, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class ClusterServiceTest extends AbstractConsoleTest {
	@Mock
	private ClusterDao mockClusterDao;
	@InjectMocks
	private ClusterServiceImpl clusterService;

	@Test
	public void testLoad() {
		ClusterTbl target_result = new ClusterTbl().setId(1).setClusterName("cluster1")
				.setClusterLastModifiedTime("1234567");

		assertEquals(clusterService.find("cluster1").getId(), target_result.getId());
		assertEquals(clusterService.find("cluster1").getClusterName(), target_result.getClusterName());
	}

	@Before
	public void initMockData() throws Exception {
		when(mockClusterDao.findClusterByClusterName("cluster1"))
				.thenReturn(new ClusterTbl().setId(1).setClusterName("cluster1").setClusterLastModifiedTime("1234567"));
	}
}
