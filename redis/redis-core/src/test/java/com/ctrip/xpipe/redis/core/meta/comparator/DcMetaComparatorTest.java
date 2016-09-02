package com.ctrip.xpipe.redis.core.meta.comparator;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.meta.MetaClone;

/**
 * @author wenchao.meng
 *
 * Sep 2, 2016
 */
public class DcMetaComparatorTest extends AbstractComparatorTest{
	
	private DcMeta current, future;
	
	@Before
	public void beforeDcMetaComparatorTest(){
		
		current = getDc();
		future = MetaClone.clone(current);
	}
	
	@Test
	public void testEquals(){
		
		DcMetaComparator dcMetaComparator = new DcMetaComparator(current, future);
		dcMetaComparator.compare();
		
		Assert.assertEquals(0, dcMetaComparator.getRemoved().size());
		Assert.assertEquals(0, dcMetaComparator.getAdded().size());
		Assert.assertEquals(0, dcMetaComparator.getMofified().size());
	}

	@Test
	public void testAdded(){
		ClusterMeta cluster = differentCluster(current);
		future.addCluster(cluster);

		DcMetaComparator dcMetaComparator = new DcMetaComparator(current, future);
		dcMetaComparator.compare();

		Assert.assertEquals(0, dcMetaComparator.getRemoved().size());
		Assert.assertEquals(1, dcMetaComparator.getAdded().size());
		Assert.assertEquals(cluster, dcMetaComparator.getAdded().toArray()[0]);
		Assert.assertEquals(0, dcMetaComparator.getMofified().size());

	}

	protected ClusterMeta differentCluster(DcMeta current) {
		ClusterMeta result = new ClusterMeta();
		result.setId(randomString());
		return result;
	}

	@Test
	public void testDelted(){

		ClusterMeta cluster = differentCluster(current);
		current.addCluster(cluster);

		DcMetaComparator dcMetaComparator = new DcMetaComparator(current, future);
		dcMetaComparator.compare();

		Assert.assertEquals(1, dcMetaComparator.getRemoved().size());
		Assert.assertEquals(cluster, dcMetaComparator.getRemoved().toArray()[0]);
		Assert.assertEquals(0, dcMetaComparator.getAdded().size());
		Assert.assertEquals(0, dcMetaComparator.getMofified().size());
	}

	@Test
	public void testModified(){
		
		ClusterMeta clusterMeta = (ClusterMeta) future.getClusters().values().toArray()[0];
		
		clusterMeta.addShard(differentShard(clusterMeta));
		
		DcMetaComparator dcMetaComparator = new DcMetaComparator(current, future);
		dcMetaComparator.compare();

		Assert.assertEquals(0, dcMetaComparator.getRemoved().size());
		Assert.assertEquals(0, dcMetaComparator.getAdded().size());
		Assert.assertEquals(1, dcMetaComparator.getMofified().size());
		
		ClusterMetaComparator comparator = (ClusterMetaComparator) dcMetaComparator.getMofified().toArray()[0];
		Assert.assertEquals(clusterMeta.getId(), comparator.getCurrent().getId());
		Assert.assertEquals(1, comparator.getAdded().size());
		
		
	}

	

}
