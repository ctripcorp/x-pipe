package com.ctrip.xpipe.redis.keeper.cluster;

import org.junit.Test;

import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;

/**
 * @author marsqing
 *
 *         May 25, 2016 11:32:54 AM
 */
public class LeaderElectorTest {

	@Test
	public void test1() throws Exception {
		final String leaderElectionID = "127.0.0.1:11111";
		ElectContext ctx = new ElectContext("/keepers/cluster01/shard01", leaderElectionID);
		new LeaderElector(new DefaultKeeperConfig(), ctx).start();

		System.in.read();
	}

}
