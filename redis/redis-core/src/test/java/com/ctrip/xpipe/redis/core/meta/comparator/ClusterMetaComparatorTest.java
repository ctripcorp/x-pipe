package com.ctrip.xpipe.redis.core.meta.comparator;


import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.SourceMeta;
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

		assertFalse(clusterMetaComparator.isConfigChange());

		Assert.assertEquals(1, clusterMetaComparator.getAdded().size());
		Assert.assertEquals(shard, clusterMetaComparator.getAdded().toArray()[0]);
		
		Assert.assertEquals(0, clusterMetaComparator.getRemoved().size());
		Assert.assertEquals(0, clusterMetaComparator.getMofified().size());
	}

	@Test
	public void testSourceShardAdded() {

		SourceMeta source = differentSourceShard();
		ShardMeta shard = differentShard(current);
		source.addShard(shard);
		future.addSource(source);

		ClusterMetaComparator clusterMetaComparator = new ClusterMetaComparator(current, future);
		clusterMetaComparator.compare();

		assertFalse(clusterMetaComparator.isConfigChange());

		Assert.assertEquals(1, clusterMetaComparator.getAdded().size());
		Assert.assertEquals(shard, clusterMetaComparator.getAdded().toArray()[0]);

		Assert.assertEquals(0, clusterMetaComparator.getRemoved().size());
		Assert.assertEquals(0, clusterMetaComparator.getMofified().size());
	}

	
	@Test
	public void testEqual(){

		ClusterMetaComparator clusterMetaComparator = new ClusterMetaComparator(current, future);
		clusterMetaComparator.compare();

		assertFalse(clusterMetaComparator.isConfigChange());

		Assert.assertEquals(0, clusterMetaComparator.getAdded().size());
		Assert.assertEquals(0, clusterMetaComparator.getRemoved().size());
		Assert.assertEquals(0, clusterMetaComparator.getMofified().size());
		
	}

	@Test
	public void testSourceShardEqual() {

		ShardMeta shard = differentShard(current);

		SourceMeta source = differentSourceShard();
		SourceMeta source2 = differentSourceShard();

		source.addShard(shard);
		source2.addShard(shard);

		current.addSource(source);
		future.addSource(source2);

		ClusterMetaComparator clusterMetaComparator = new ClusterMetaComparator(current, future);
		clusterMetaComparator.compare();

		assertFalse(clusterMetaComparator.isConfigChange());

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

		assertFalse(clusterMetaComparator.isConfigChange());

		Assert.assertEquals(0, clusterMetaComparator.getAdded().size());
		
		Assert.assertEquals(1, clusterMetaComparator.getRemoved().size());
		Assert.assertEquals(shard, clusterMetaComparator.getRemoved().toArray()[0]);
		
		Assert.assertEquals(0, clusterMetaComparator.getMofified().size());
	}

	@Test
	public void testSourceShardRemoved() {

		SourceMeta sourceMeta = differentSourceShard();
		ShardMeta shard = differentShard(current);

		sourceMeta.addShard(shard);
		current.addSource(sourceMeta);

		ClusterMetaComparator clusterMetaComparator = new ClusterMetaComparator(current, future);
		clusterMetaComparator.compare();

		assertFalse(clusterMetaComparator.isConfigChange());

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
	public void testSourceShardModified(){

		SourceMeta sourceMeta = differentSourceShard();
		ShardMeta shardMeta = differentShard(current);
		sourceMeta.addShard(shardMeta);
		current.addSource(sourceMeta);

		SourceMeta sourceMeta2 = differentSourceShard();
		ShardMeta shardMeta2 = new ShardMeta(shardMeta.getId());
		shardMeta2.addKeeper(differentKeeper(shardMeta2));
		sourceMeta2.addShard(shardMeta2);
		future.addSource(sourceMeta2);

		ClusterMetaComparator clusterMetaComparator = new ClusterMetaComparator(current, future);
		clusterMetaComparator.compare();

		Assert.assertEquals(0, clusterMetaComparator.getAdded().size());

		Assert.assertEquals(0, clusterMetaComparator.getRemoved().size());

		Assert.assertEquals(1, clusterMetaComparator.getMofified().size());
		ShardMetaComparator comparator = (ShardMetaComparator) clusterMetaComparator.getMofified().toArray()[0];
		Assert.assertEquals(shardMeta.getId(), comparator.getCurrent().getId());
	}

	@Test
	public void testConfigChanged() {
		current.setDcs("dc1");
		future.setDcs("dc1,dc2");
		ClusterMetaComparator clusterMetaComparator = new ClusterMetaComparator(current, future);
		clusterMetaComparator.compare();

		Assert.assertTrue(clusterMetaComparator.isConfigChange());
		Assert.assertTrue(clusterMetaComparator.getAdded().isEmpty());
		Assert.assertTrue(clusterMetaComparator.getRemoved().isEmpty());
		Assert.assertTrue(clusterMetaComparator.getMofified().isEmpty());
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
