package com.ctrip.xpipe.redis.core.meta.comparator;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
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
		result.setDbId(Math.abs(randomLong()));
		return result;
	}

	@Test
	public void testDeleted(){

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

	@Test
	public void testModifyRedisConfig(){

		ClusterMeta clusterMeta = (ClusterMeta) future.getClusters().values().toArray()[0];

		ShardMeta shardMeta = (ShardMeta) clusterMeta.getShards().values().toArray()[0];

		RedisMeta redisMeta = shardMeta.getRedises().get(0);
		redisMeta.setPort(redisMeta.getPort() + 10000);

		DcMetaComparator dcMetaComparator = new DcMetaComparator(current, future);
		dcMetaComparator.compare();

		Assert.assertEquals(0, dcMetaComparator.getRemoved().size());
		Assert.assertEquals(0, dcMetaComparator.getAdded().size());
		Assert.assertEquals(1, dcMetaComparator.getMofified().size());

		ClusterMetaComparator comparator = (ClusterMetaComparator) dcMetaComparator.getMofified().toArray()[0];
		Assert.assertEquals(clusterMeta.getId(), comparator.getCurrent().getId());
		Assert.assertEquals(0, comparator.getAdded().size());
		Assert.assertEquals(0, comparator.getRemoved().size());
		Assert.assertEquals(1, comparator.getMofified().size());

		ShardMetaComparator shardMetaComparator = (ShardMetaComparator) comparator.getMofified().toArray()[0];
		Assert.assertEquals(1, shardMetaComparator.getAdded().size());
		Assert.assertEquals(1, shardMetaComparator.getRemoved().size());
		Assert.assertEquals(0, shardMetaComparator.getMofified().size());

		logger.debug("{}", dcMetaComparator);
	}

	@Test
	public void testClusterNameChange() {
		ClusterMeta clusterMeta = future.getClusters().values().iterator().next();
		Long clusterDbId = clusterMeta.getDbId();
		clusterMeta.setId(randomString(10));
		DcMetaComparator dcMetaComparator = new DcMetaComparator(current, future);
		dcMetaComparator.compare();

		Assert.assertEquals(0, dcMetaComparator.getAdded().size());
		Assert.assertEquals(0, dcMetaComparator.getRemoved().size());
		Assert.assertEquals(1, dcMetaComparator.getMofified().size());

		ClusterMetaComparator comparator = (ClusterMetaComparator)  dcMetaComparator.getMofified().iterator().next();
		Assert.assertEquals(comparator.getCurrent().getDbId(), clusterDbId);
		Assert.assertEquals(comparator.getFuture().getDbId(), clusterDbId);
		Assert.assertNotEquals(comparator.getCurrent().getId(), comparator.getFuture().getId());
		Assert.assertEquals(0, comparator.getAdded().size());
		Assert.assertEquals(0, comparator.getRemoved().size());
		Assert.assertEquals(0, comparator.getMofified().size());
	}


}
