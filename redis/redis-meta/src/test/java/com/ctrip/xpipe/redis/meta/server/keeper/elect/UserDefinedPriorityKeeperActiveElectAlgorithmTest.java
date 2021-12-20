package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;


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
		
		Assert.assertEquals(priority.get(0), algorithm.select(getClusterDbId(), getShardDbId(), priority));
		
		List<KeeperMeta> onlyOne = new LinkedList<>();
		onlyOne.add(priority.get(1));
		Assert.assertEquals(priority.get(1), algorithm.select(getClusterDbId(), getShardDbId(), onlyOne));

		List<KeeperMeta> none = new LinkedList<>();
		none.add(differentKeeper(priority));
		Assert.assertEquals(none.get(0), algorithm.select(getClusterDbId(), getShardDbId(), none));

		
		List<KeeperMeta> reverse = new LinkedList<>(priority);
		Collections.reverse(reverse);
		Assert.assertEquals(priority.get(0), algorithm.select(getClusterDbId(), getShardDbId(), reverse));
	}

}
