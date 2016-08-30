package com.ctrip.xpipe.redis.meta.server.keeper.impl;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;

/**
 * @author wenchao.meng
 *
 * Aug 30, 2016
 */
public class LeaderWatchedShardsTest extends AbstractMetaServerTest{
	
	@Test
	public void test(){
		
		String clusterId = "cluster1", shardId = "shard1";
		
		LeaderWatchedShards leaderWatchedShards = new LeaderWatchedShards();
		
		Assert.assertTrue(leaderWatchedShards.addIfNotExist(clusterId, shardId));
		Assert.assertFalse(leaderWatchedShards.addIfNotExist(clusterId, shardId));
		
		Assert.assertTrue(leaderWatchedShards.hasCluster(clusterId));
		
		leaderWatchedShards.removeByClusterId(clusterId);
		
		Assert.assertFalse(leaderWatchedShards.hasCluster(clusterId));
		
		Assert.assertTrue(leaderWatchedShards.addIfNotExist(clusterId, shardId));
		
	}

}
