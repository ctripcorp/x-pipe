package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster;

import com.ctrip.xpipe.tuple.Pair;

/**
 * @author wenchao.meng
 *
 * Nov 4, 2016
 */
public interface KeeperMasterChooserAlgorithm{
	
	Pair<String, Integer> choose();

}
