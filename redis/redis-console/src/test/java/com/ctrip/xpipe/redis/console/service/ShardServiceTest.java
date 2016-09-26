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
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.model.ShardTblDao;
import com.ctrip.xpipe.redis.console.model.ShardTblEntity;

/**
 * @author shyin
 *
 *         Sep 26, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class ShardServiceTest extends AbstractConsoleTest {
	@Mock
	private ShardTblDao mockedShardTblDao;
	@InjectMocks
	private ShardService shardService;

	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);

		try {
			generateShardMockData();
		} catch (Exception e) {
			logger.error("Generate Dc mock data failed.", e);
		}
	}

	@Test
	public void testShardService() {
		ShardTbl target_result = new ShardTbl().setId(1).setClusterId(1).setShardName("shard1");

		assertEquals(shardService.load("cluster1", "shard1").getId(), target_result.getId());
		assertEquals(shardService.load("cluster1", "shard1").getClusterId(), target_result.getClusterId());
		assertEquals(shardService.load("cluster1", "shard1").getShardName(), target_result.getShardName());
	}

	private void generateShardMockData() throws DalException {
		when(mockedShardTblDao.findShard("cluster1", "shard1", ShardTblEntity.READSET_FULL))
				.thenReturn(new ShardTbl().setId(1).setClusterId(1).setShardName("shard1"));
	}
}
