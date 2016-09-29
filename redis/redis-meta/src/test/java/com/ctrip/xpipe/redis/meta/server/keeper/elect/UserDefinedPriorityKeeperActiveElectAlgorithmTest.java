package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;


/**
 * @author wenchao.meng
 *
 * Sep 9, 2016
 */
public class UserDefinedPriorityKeeperActiveElectAlgorithmTest extends AbstractMetaServerTest{
	
	@Test
	public void test(){
		
		List<KeeperMeta> priority = getDcKeepers(getDc(), getClusterId(), getShardId());
		
		UserDefinedPriorityKeeperActiveElectAlgorithm algorithm = new UserDefinedPriorityKeeperActiveElectAlgorithm(priority);
		
		Assert.assertEquals(priority.get(0), algorithm.select(getClusterId(), getShardId(), priority));
		
		List<KeeperMeta> onlyOne = new LinkedList<>();
		onlyOne.add(priority.get(1));
		Assert.assertEquals(priority.get(1), algorithm.select(getClusterId(), getShardId(), onlyOne));

		List<KeeperMeta> none = new LinkedList<>();
		none.add(differentKeeper(priority));
		Assert.assertEquals(none.get(0), algorithm.select(getClusterId(), getShardId(), none));

		
		List<KeeperMeta> reverse = new LinkedList<>(priority);
		Collections.reverse(reverse);
		Assert.assertEquals(priority.get(0), algorithm.select(getClusterId(), getShardId(), reverse));
	}

}
