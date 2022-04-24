package com.ctrip.xpipe.redis.core.meta.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaException;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategy;
import com.ctrip.xpipe.redis.core.route.impl.Crc32HashRouteChooseStrategy;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.assertj.core.util.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wenchao.meng
 *
 *         Jun 23, 2016
 */
public class DefaultXpipeMetaManagerTest extends AbstractRedisTest {

	private DefaultXpipeMetaManager metaManager;

	private String dc = "jq", clusterId1 = "cluster1", clusterId2 = "cluster2", shardId = "shard1";
	@SuppressWarnings("unused")
	private String dcBak1 = "fq", dcBak2 = "fra";

	@Before
	public void beforeDefaultFileDaoTest() throws Exception {

		metaManager = (DefaultXpipeMetaManager) DefaultXpipeMetaManager.buildFromFile("file-dao-test.xml");
		add(metaManager);
	}

	@Test
	public void testMetaRoutes() {

		List<RouteMeta> routeMetas = metaManager.metaRoutes(dcBak2);
		Assert.assertEquals(7, routeMetas.size());
		routeMetas.sort((o1, o2) -> o1.getId() - o2.getId());
		Assert.assertEquals(new Integer(1), routeMetas.get(0).getId());
		Assert.assertEquals(new Integer(2), routeMetas.get(1).getId());
		Assert.assertEquals(new Integer(3), routeMetas.get(2).getId());
		Assert.assertEquals(new Integer(4), routeMetas.get(3).getId());
		Assert.assertEquals(new Integer(5), routeMetas.get(4).getId());
		Assert.assertEquals(new Integer(9), routeMetas.get(5).getId());
		Assert.assertEquals(new Integer(10), routeMetas.get(6).getId());

		List<RouteMeta> routeMetas2 = metaManager.metaRoutes(dcBak1);
		Assert.assertEquals(0, routeMetas2.size());
	}

	@Test
	public void testGetSpecificActiveDcClusters() {

		List<ClusterMeta> specificActiveDcClusters1 = metaManager.getSpecificActiveDcClusters(dcBak2, dc);
		Assert.assertEquals(2, specificActiveDcClusters1.size());
		Assert.assertEquals(clusterId1, specificActiveDcClusters1.get(0).getId());
		Assert.assertEquals(clusterId2, specificActiveDcClusters1.get(1).getId());


		List<ClusterMeta> specificActiveDcClusters2 = metaManager.getSpecificActiveDcClusters(dcBak2, dcBak1);
		Assert.assertEquals(1, specificActiveDcClusters2.size());
		Assert.assertEquals("cluster3", specificActiveDcClusters2.get(0).getId());

		//empty test
		List<ClusterMeta> specificActiveDcClusters3 = metaManager.getSpecificActiveDcClusters(dcBak2, "empty");
		Assert.assertEquals(0, specificActiveDcClusters3.size());
	}

	@Test
	public void testRandom() {

		int total = 100;
		List<Integer> integers = new LinkedList<>();
		for (int i = 0; i < total; i++) {
			integers.add(i);
		}

		HashMap<Integer, AtomicInteger> map = new HashMap<>();
		for (int i = 0; i < (1 << 20); i++) {
			Integer random = metaManager.random(integers);
			AtomicInteger put = map.putIfAbsent(random, new AtomicInteger(1));
			if (put != null) {
				put.incrementAndGet();
			}
		}
		logger.debug("{}", map);
		Assert.assertEquals(total, map.size());

		Assert.assertNull(metaManager.random(new LinkedList<>()));
	}

	@Test
	public void findShard() {

		XpipeMetaManager.MetaDesc metaDesc = metaManager.findMetaDesc(new HostPort("127.0.0.1", 8000));

		Assert.assertEquals("jq", metaDesc.getDcId());
		Assert.assertEquals("cluster1", metaDesc.getClusterId());
		Assert.assertEquals("shard1", metaDesc.getShardId());


		metaDesc = metaManager.findMetaDesc(new HostPort("127.0.0.1", 6000));
		Assert.assertEquals("jq", metaDesc.getDcId());
		Assert.assertEquals("cluster1", metaDesc.getClusterId());
		Assert.assertEquals("shard1", metaDesc.getShardId());
	}

	@Test
	public void testActiveDc() {

		Assert.assertEquals(dc, metaManager.getActiveDc(clusterId1, shardId));
		Assert.assertEquals(dc, metaManager.getActiveDc(clusterId1, null));
	}

	@Test
	public void testGetRedisMaster() {

		Pair<String, RedisMeta> redisMaster = metaManager.getRedisMaster("cluster1", "shard1");
		Assert.assertEquals("jq", redisMaster.getKey());
	}

	@Test
	public void testChangePrimaryDc() {

		String primaryDc = metaManager.getActiveDc(clusterId1, shardId);
		Set<String> backupDcs = metaManager.getBackupDcs(clusterId1, shardId);

		metaManager.primaryDcChanged(dc, clusterId1, shardId, primaryDc);

		Assert.assertEquals(primaryDc, metaManager.getActiveDc(clusterId1, shardId));

		String newPrimary = backupDcs.iterator().next();

		metaManager.primaryDcChanged(dc, clusterId1, shardId, newPrimary);

		Assert.assertEquals(newPrimary, metaManager.getActiveDc(clusterId1, shardId));

		Assert.assertTrue(metaManager.getBackupDcs(clusterId1, shardId).contains(primaryDc));

	}

	@Test
	public void testGetSentinel() {

		SentinelMeta sentinelMeta = metaManager.getSentinel(dc, clusterId1, shardId);
		Assert.assertEquals("127.0.0.1:17171,127.0.0.1:17171", sentinelMeta.getAddress());
	}

	@Test
	public void testGetBackupDcs() {

		Set<String> real = metaManager.getBackupDcs(clusterId1, shardId);

		logger.info("[testGetBackupDcs]{}", real);

		Set<String> expected = new HashSet<>();
		expected.add("oy");
		expected.add("fq");
		expected.add("fra");
		Assert.assertEquals(expected, real);
		;

		try {
			metaManager.getBackupDcs(randomString(), shardId);
			Assert.fail();
		} catch (Exception e) {

		}

	}

	@Test
	public void testHas() {

		Assert.assertTrue(metaManager.hasCluster(dc, clusterId1));

		Assert.assertFalse(metaManager.hasCluster(dc, randomString()));

		Assert.assertFalse(metaManager.hasCluster(randomString(), clusterId1));


		Assert.assertTrue(metaManager.hasShard(dc, clusterId1, shardId));

		Assert.assertFalse(metaManager.hasShard(dc, clusterId1, randomString()));

		Assert.assertFalse(metaManager.hasShard(dc, randomString(), shardId));

		Assert.assertFalse(metaManager.hasShard(randomString(), clusterId1, shardId));
	}

	@Test
	public void testSetKeeperAlive() {

		List<KeeperMeta> allSurvice = metaManager.getAllSurviveKeepers(dc, clusterId1, shardId);
		logger.info("[testSetKeeperAlive][allAlive]{}", allSurvice);
		Assert.assertEquals(0, allSurvice.size());

		List<KeeperMeta> allKeepers = metaManager.getKeepers(dc, clusterId1, shardId);
		for (KeeperMeta allOne : allKeepers) {
			allOne.setSurvive(true);
		}

		allSurvice = metaManager.getAllSurviveKeepers(dc, clusterId1, shardId);
		Assert.assertEquals(0, allSurvice.size());

		metaManager.setSurviveKeepers(dc, clusterId1, shardId, allKeepers);

		allSurvice = metaManager.getAllSurviveKeepers(dc, clusterId1, shardId);
		Assert.assertEquals(allKeepers.size(), allSurvice.size());

		try {
			KeeperMeta nonExist = createNonExistKeeper(allKeepers);
			allKeepers.add(nonExist);
			metaManager.setSurviveKeepers(dc, clusterId1, shardId, allKeepers);
			Assert.fail();
		} catch (IllegalArgumentException e) {

		}

	}

	@Test
	public void testUpdateKeeperActive() throws MetaException {

		List<KeeperMeta> backups = metaManager.getKeeperBackup(dc, clusterId1, shardId);

		Assert.assertNotNull(metaManager.getKeeperActive(dc, clusterId1, shardId));
		;

		KeeperMeta backup = backups.get(0);

		metaManager.updateKeeperActive(dc, clusterId1, shardId, backups.get(0));

		KeeperMeta newActive = metaManager.getKeeperActive(dc, clusterId1, shardId);
		Assert.assertEquals(backup.getIp(), newActive.getIp());
		Assert.assertEquals(backup.getPort(), newActive.getPort());

		metaManager.updateKeeperActive(dc, clusterId1, shardId, new KeeperMeta());
		Assert.assertNull(metaManager.getKeeperActive(dc, clusterId1, shardId));
	}

	@Test
	public void testUpdateRedisMaster() throws MetaException {

		Pair<String, RedisMeta> redisMaster = metaManager.getRedisMaster(clusterId1, shardId);
		Assert.assertEquals(redisMaster.getKey(), "jq");
		boolean result = metaManager.updateRedisMaster(redisMaster.getKey(), clusterId1, shardId,
				redisMaster.getValue());
		Assert.assertTrue(!result);

		KeeperMeta activeKeeper = null;
		for (KeeperMeta keeperMeta : metaManager.getKeepers(dc, clusterId1, shardId)) {
			if (keeperMeta.getMaster()
					.equals(String.format("%s:%d", redisMaster.getValue().getIp(), redisMaster.getValue().getPort()))) {
				activeKeeper = keeperMeta;
			}
		}
		Assert.assertNotNull(activeKeeper);

		for (RedisMeta redis : metaManager.getRedises(dc, clusterId1, shardId)) {

			if (!redis.equals(redisMaster.getValue())) {
				String master = String.format("%s:%d", redis.getIp(), redis.getPort());
				Assert.assertNotEquals(activeKeeper.getMaster(), master);
				result = metaManager.updateRedisMaster(redisMaster.getKey(), clusterId1, shardId, redis);
				Assert.assertTrue(result);

				KeeperMeta active = metaManager.getKeeperActive(redisMaster.getKey(), clusterId1, shardId);
				Assert.assertEquals(active.getMaster(), master);
			}
		}
	}

	@Test
	public void testChooseRouteOneWay() {
		List<String> peerDcs = Lists.newArrayList("jq");
		String currentDc = "fra";
		RouteChooseStrategy strategy = new Crc32HashRouteChooseStrategy(clusterId1);
		RouteMeta routeMeta1 = new RouteMeta().setId(1);
		RouteMeta routeMeta2 = new RouteMeta().setId(2);
		RouteMeta routeMeta3 = new RouteMeta().setId(3);
		RouteMeta routeMeta4 = new RouteMeta().setId(4);
		RouteMeta routeMeta9 = new RouteMeta().setId(9);

		//test same org-id
		Map<String, RouteMeta> chooseRoute = metaManager.chooseMetaRoute(currentDc, peerDcs, 1, null, strategy);
		Assert.assertEquals(1, chooseRoute.size());
		Assert.assertEquals(Sets.newHashSet("jq"), chooseRoute.keySet());
		Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta1, routeMeta2)).getId(), chooseRoute.get("jq").getId());

		//test default org-id
		chooseRoute = metaManager.chooseMetaRoute(currentDc, peerDcs, 0, null, strategy);
		Assert.assertEquals(1, chooseRoute.size());
		Assert.assertEquals(Sets.newHashSet("jq"), chooseRoute.keySet());
		Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta4)).getId(), chooseRoute.get("jq").getId());


		//test cluster designated route
		chooseRoute = metaManager.chooseMetaRoute(currentDc, peerDcs, 1, Maps.newHashMap("jq", Lists.newArrayList(routeMeta3, routeMeta9)), strategy);
		Assert.assertEquals(1, chooseRoute.size());
		Assert.assertEquals(Sets.newHashSet("jq"), chooseRoute.keySet());
		Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta3)).getId(), chooseRoute.get("jq").getId());

		//test cluster designated route wrong
		chooseRoute = metaManager.chooseMetaRoute(currentDc, peerDcs, 1, Maps.newHashMap("jq", Lists.newArrayList(routeMeta9)), strategy);
		Assert.assertEquals(1, chooseRoute.size());
		Assert.assertEquals(Sets.newHashSet("jq"), chooseRoute.keySet());
		Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta1, routeMeta2)).getId(), chooseRoute.get("jq").getId());
	}

	@Test
	public void testChooseRouteBiDirection() {
		List<String> peerDcs = Lists.newArrayList("jq", "OY");
		String currentDc = "fra";
		RouteChooseStrategy strategy = new Crc32HashRouteChooseStrategy(clusterId1);
		RouteMeta routeMeta1 = new RouteMeta().setId(1);
		RouteMeta routeMeta2 = new RouteMeta().setId(2);
		RouteMeta routeMeta3 = new RouteMeta().setId(3);
		RouteMeta routeMeta4 = new RouteMeta().setId(4);
		RouteMeta routeMeta9 = new RouteMeta().setId(9);
		RouteMeta routeMeta10 = new RouteMeta().setId(10);

		//test same org-id
		Map<String, RouteMeta> chooseRoute = metaManager.chooseMetaRoute(currentDc, peerDcs, 1, null, strategy);
		Assert.assertEquals(2, chooseRoute.size());
		Assert.assertEquals(Sets.newHashSet("jq", "oy"), chooseRoute.keySet());
		Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta1, routeMeta2)).getId(), chooseRoute.get("jq").getId());
		Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta9)).getId(), chooseRoute.get("oy").getId());

		//test default org-id
		chooseRoute = metaManager.chooseMetaRoute(currentDc, peerDcs, 0, null, strategy);
		Assert.assertEquals(2, chooseRoute.size());
		Assert.assertEquals(Sets.newHashSet("jq", "oy"), chooseRoute.keySet());
		Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta4)).getId(), chooseRoute.get("jq").getId());
		Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta10)).getId(), chooseRoute.get("oy").getId());


		//test cluster designated route
		chooseRoute = metaManager.chooseMetaRoute(currentDc, peerDcs, 1, Maps.newHashMap("oy", Lists.newArrayList(routeMeta10)), strategy);
		Assert.assertEquals(2, chooseRoute.size());
		Assert.assertEquals(Sets.newHashSet("jq", "oy"), chooseRoute.keySet());
		Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta1, routeMeta2)).getId(), chooseRoute.get("jq").getId());
		Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta10)).getId(), chooseRoute.get("oy").getId());

		//test cluster designated route wrong
		chooseRoute = metaManager.chooseMetaRoute(currentDc, peerDcs, 1, Maps.newHashMap("oy", Lists.newArrayList(routeMeta3)), strategy);
		Assert.assertEquals(2, chooseRoute.size());
		Assert.assertEquals(Sets.newHashSet("jq", "oy"), chooseRoute.keySet());
		Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta1, routeMeta2)).getId(), chooseRoute.get("jq").getId());
		Assert.assertEquals(strategy.choose(Lists.newArrayList(routeMeta9)).getId(), chooseRoute.get("oy").getId());

		Map<String, List<RouteMeta>> designatedRoute = new HashMap<>();
		designatedRoute.put("oy", Lists.newArrayList(routeMeta10, routeMeta3));
		designatedRoute.put("jq", Lists.newArrayList(routeMeta1));
		chooseRoute = metaManager.chooseMetaRoute(currentDc, peerDcs, 0, designatedRoute, strategy);
		Assert.assertEquals(2, chooseRoute.size());
		Assert.assertEquals(Sets.newHashSet("jq", "oy"), chooseRoute.keySet());
		Assert.assertEquals(routeMeta1.getId(), chooseRoute.get("jq").getId());
		Assert.assertEquals(routeMeta10.getId(), chooseRoute.get("oy").getId());
	}

	@Test
	public void testChooseRouteWithoutRoute() {
		List<String> peerDcs = Lists.newArrayList( "oy", "fra");
		String currentDc = "jq";
		RouteChooseStrategy strategy = new Crc32HashRouteChooseStrategy(clusterId1);

		Map<String, RouteMeta> chooseRoute = metaManager.chooseMetaRoute(currentDc, peerDcs, 1, null, strategy);
		Assert.assertEquals(0, chooseRoute.size());
	}
}
