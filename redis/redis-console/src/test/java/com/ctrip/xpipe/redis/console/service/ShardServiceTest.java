package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.model.ShardTblDao;
import com.ctrip.xpipe.redis.console.model.ShardTblEntity;
import com.ctrip.xpipe.redis.console.service.impl.ShardServiceImpl;
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
public class ShardServiceTest extends AbstractConsoleTest {
	@Mock
	private ShardTblDao mockedShardTblDao;
	@InjectMocks
	private ShardServiceImpl shardService;

	@Test
	public void testShardService() {
		ShardTbl target_result = new ShardTbl().setId(1).setClusterId(1).setShardName("shard1");

		assertEquals(shardService.find("cluster1", "shard1").getId(), target_result.getId());
		assertEquals(shardService.find("cluster1", "shard1").getClusterId(), target_result.getClusterId());
		assertEquals(shardService.find("cluster1", "shard1").getShardName(), target_result.getShardName());
	}

	@Before
	public void initMockData() throws Exception {
		when(mockedShardTblDao.findShard("cluster1", "shard1", ShardTblEntity.READSET_FULL))
				.thenReturn(new ShardTbl().setId(1).setClusterId(1).setShardName("shard1"));
	}
}
