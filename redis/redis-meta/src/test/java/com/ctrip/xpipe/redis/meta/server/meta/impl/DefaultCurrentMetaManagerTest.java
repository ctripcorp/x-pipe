package com.ctrip.xpipe.redis.meta.server.meta.impl;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.unidal.tuple.Pair;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.meta.comparator.DcMetaComparator;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerContextTest;


/**
 * @author wenchao.meng
 *
 * Aug 31, 2016
 */
public class DefaultCurrentMetaManagerTest extends AbstractMetaServerContextTest{
	
	private DefaultCurrentMetaManager   currentMetaServerMetaManager;
	
	@Before
	public void beforeDefaultCurrentMetaServerMetaManagerTest(){
		currentMetaServerMetaManager = getBean(DefaultCurrentMetaManager.class);
	}
	
	@Test
	public void testAdd(){
		
		DcMetaComparator add = DcMetaComparator.buildClusterChanged(null, randomCluster());
		currentMetaServerMetaManager.update(add, null);
		
	}
	
	
	private ClusterMeta randomCluster() {
		// TODO Auto-generated method stub
		return null;
	}

	@Test
	public void testAddOrRemove(){
		
		Set<Integer> future = new HashSet<>();
		future.add(1);future.add(2);future.add(3);
		
		Set<Integer> current = new HashSet<>();
		current.add(1);current.add(2);current.add(4);
		
		Pair<Set<Integer>, Set<Integer>> result = currentMetaServerMetaManager.getAddAndRemove(future, current);
		
		Assert.assertEquals(3, future.size());
		Assert.assertEquals(3, current.size());
		
		Assert.assertEquals(1, result.getKey().size());
		Assert.assertEquals(3, result.getKey().toArray()[0]);

		Assert.assertEquals(1, result.getValue().size());
		Assert.assertEquals(4, result.getValue().toArray()[0]);
	}
	
	@Override
	protected boolean isStartZk() {
		return false;
	}

}
