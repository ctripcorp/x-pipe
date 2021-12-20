package com.ctrip.xpipe.redis.core.meta.comparator;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaClone;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;



/**
 * @author wenchao.meng
 *
 * Sep 2, 2016
 */
public class ShardMetaComparatorTest extends AbstractComparatorTest{
	
	private ShardMeta current;
	private ShardMeta future;
	
	@Before
	public void beforeShardMetaComparatorTest(){
		current = getShard();
		future = MetaClone.clone(current);
		
	}
	
	@Test
	public void testRemove(){
		//equal
		KeeperMeta removed = differentKeeper(current); 
		current.addKeeper(removed);
		
		ShardMetaComparator comparator = new ShardMetaComparator(current, future);
		comparator.compare();
		
		Assert.assertEquals(0, comparator.getAdded().size());
		
		Assert.assertEquals(1, comparator.getRemoved().size());
		Assert.assertEquals(removed, comparator.getRemoved().toArray()[0]);
		
		Assert.assertEquals(0, comparator.getMofified().size());
	}


	@Test
	public void testAdd(){
		//equal
		KeeperMeta added = differentKeeper(current); 
		future.addKeeper(added);
		ShardMetaComparator comparator = new ShardMetaComparator(current, future);
		comparator.compare();
		
		Assert.assertEquals(1, comparator.getAdded().size());
		Assert.assertEquals(added, comparator.getAdded().toArray()[0]);
		
		Assert.assertEquals(0, comparator.getRemoved().size());
		Assert.assertEquals(0, comparator.getMofified().size());
	}
	


	@Test
	public void testEquals(){
		//equal
		ShardMetaComparator comparator = new ShardMetaComparator(current, future);
		comparator.compare();
		
		Assert.assertEquals(0, comparator.getAdded().size());
		Assert.assertEquals(0, comparator.getRemoved().size());
		Assert.assertEquals(0, comparator.getMofified().size());
	}


	@Test
	public void testEqualsWithSentinelIdChanged(){

		//equal

		long currentSentinelId = future.getSentinelId() == null ? 0 : future.getSentinelId();
		future.setSentinelId( currentSentinelId + 100 );

		ShardMetaComparator comparator = new ShardMetaComparator(current, future);
		comparator.compare();

		Assert.assertEquals(0, comparator.getAdded().size());
		Assert.assertEquals(0, comparator.getRemoved().size());
		Assert.assertEquals(0, comparator.getMofified().size());
	}


}
