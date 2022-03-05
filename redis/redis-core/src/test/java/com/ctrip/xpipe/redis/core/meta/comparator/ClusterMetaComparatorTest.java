package com.ctrip.xpipe.redis.core.meta.comparator;


import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaClone;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;


/**
 * @author wenchao.meng
 *
 * Sep 2, 2016
 */
public class ClusterMetaComparatorTest extends AbstractComparatorTest{
	
	private ClusterMeta current, future;
	
	@Before
	public void beforeClusterMetaComparatorTest(){
		current = getCluster();
		future = MetaClone.clone(current);
		
	}

	@Test
	public void testAdded(){
		
		ShardMeta shard = differentShard(current);
		future.addShard(shard);
		
		ClusterMetaComparator clusterMetaComparator = new ClusterMetaComparator(current, future);
		clusterMetaComparator.compare();

		assertFalse(clusterMetaComparator.isShallowChange());

		Assert.assertEquals(1, clusterMetaComparator.getAdded().size());
		Assert.assertEquals(shard, clusterMetaComparator.getAdded().toArray()[0]);
		
		Assert.assertEquals(0, clusterMetaComparator.getRemoved().size());
		Assert.assertEquals(0, clusterMetaComparator.getMofified().size());
	}

	
	@Test
	public void testEqual(){
		
		ClusterMetaComparator clusterMetaComparator = new ClusterMetaComparator(current, future);
		clusterMetaComparator.compare();

		assertFalse(clusterMetaComparator.isShallowChange());

		Assert.assertEquals(0, clusterMetaComparator.getAdded().size());
		Assert.assertEquals(0, clusterMetaComparator.getRemoved().size());
		Assert.assertEquals(0, clusterMetaComparator.getMofified().size());
		
	}


	@Test
	public void testRemoved(){
		
		ShardMeta shard = differentShard(current);
		current.addShard(shard);
		
		ClusterMetaComparator clusterMetaComparator = new ClusterMetaComparator(current, future);
		clusterMetaComparator.compare();

		assertFalse(clusterMetaComparator.isShallowChange());

		Assert.assertEquals(0, clusterMetaComparator.getAdded().size());
		
		Assert.assertEquals(1, clusterMetaComparator.getRemoved().size());
		Assert.assertEquals(shard, clusterMetaComparator.getRemoved().toArray()[0]);
		
		Assert.assertEquals(0, clusterMetaComparator.getMofified().size());
	}

	@Test
	public void testModified(){
		
		ShardMeta shardMeta = (ShardMeta) future.getShards().values().toArray()[0];
		shardMeta.addKeeper(differentKeeper(shardMeta));

		ClusterMetaComparator clusterMetaComparator = new ClusterMetaComparator(current, future);
		clusterMetaComparator.compare();

		Assert.assertEquals(0, clusterMetaComparator.getAdded().size());
		
		Assert.assertEquals(0, clusterMetaComparator.getRemoved().size());
		
		Assert.assertEquals(1, clusterMetaComparator.getMofified().size());
		ShardMetaComparator comparator = (ShardMetaComparator) clusterMetaComparator.getMofified().toArray()[0];
		Assert.assertEquals(shardMeta.getId(), comparator.getCurrent().getId());
	}
	
	@Test
	public void testEquals(){
		
		String a1 = new String("123");
		String a2 = new String("123");
		Assert.assertTrue(EqualsBuilder.reflectionEquals(a2, a1, false));
		
		a1.hashCode();
		assertFalse(EqualsBuilder.reflectionEquals(a2, a1, false));
		
		Assert.assertTrue(EqualsBuilder.reflectionEquals(a1, a2, "hash"));
	}

}
