package com.ctrip.xpipe.redis.meta.server.keeper;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.clone.MetaCloneFacade;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.config.UnitTestServerConfig;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import com.ctrip.xpipe.tuple.Pair;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.mockito.Mockito.*;


/**
 * @author wenchao.meng
 *
 * Jan 4, 2017
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class DefaultKeeperStateChangeHandlerTest extends AbstractMetaServerTest{
	
	private DefaultKeeperStateChangeHandler handler;
	
	@Mock
	private CurrentMetaManager currentMetaManager;
	
	@Mock
	private DcMetaCache dcMetaCache;

	@Mock
	private MultiDcService multiDcService;

	private String clusterId, shardId;
	private Long clusterDbId, shardDbId;
	
	private List<KeeperMeta> keepers;

	private List<ApplierMeta> appliers;

	private RedisMeta redis;

	private Pair<String, Integer> keeperMaster;
	
	private int setStateTimeMilli = 1500;
	
	private AtomicInteger calledCount = new AtomicInteger();
	private AtomicInteger redisCall = new AtomicInteger();
	private AtomicInteger applierCall = new AtomicInteger();
	private final List<String> keeperCallOrder = Collections.synchronizedList(new ArrayList<>());

	@Before
	public void beforeDefaultKeeperStateChangeHandlerTest() throws Exception{
		
		handler = new DefaultKeeperStateChangeHandler();
		handler.setClientPool(getXpipeNettyClientKeyedObjectPool());
		handler.setcurrentMetaManager(currentMetaManager);
		handler.setDcMetaCache(dcMetaCache);
		handler.setScheduled(scheduled);
		handler.setExecutors(executors);
		handler.setMultiDcService(multiDcService);
		handler.setMetaServerConfig(new UnitTestServerConfig());

		clusterId = getClusterId();
		shardId = getShardId();
		clusterDbId = getClusterDbId();
		shardDbId = getShardDbId();
		
		keepers = createRandomKeepers(2);
		appliers = createRandomAppliers(1);
		redis = newRandomFakeRedisMeta("localhost", randomPort());


		keeperMaster = new Pair<>("localhost", randomPort());
		
		LifecycleHelper.initializeIfPossible(handler);
		LifecycleHelper.startIfPossible(handler);
		add(handler);

		startServer(keepers.get(0).getPort(), new Function<String, String>() {
			@Override
			public String apply(String s) {
				int current = calledCount.incrementAndGet();
				recordKeeperSetState(keepers.get(0).getPort(), s);
				logger.info("keeper0, callCount:{}, msg:{}", current, s);
				sleep(setStateTimeMilli);
				return "+OK\r\n";
			}
		});

		startServer(keepers.get(1).getPort(), new Function<String, String>() {

			@Override
			public String apply(String s) {
				int current = calledCount.incrementAndGet();
				recordKeeperSetState(keepers.get(1).getPort(), s);
				logger.info("keeper1, callCount:{}, msg:{}", current, s);
				return "+OK\r\n";
			}
		});

		startServer(appliers.get(0).getPort(), new Function<String, String>() {
			@Override
			public String apply(String s) {
				int current = applierCall.incrementAndGet();
				logger.info("applier1, callCount:{}, msg:{}", current, s);
				return "+OK\r\n";
			}
		});

		startServer(redis.getPort(), new Callable<String>() {
			@Override
			public String call() throws Exception {
				redisCall.incrementAndGet();
				return "+OK\r\n";
			}
		});

		when(currentMetaManager.getSurviveKeepers(clusterDbId, shardDbId)).thenReturn(keepers);
		when(currentMetaManager.getKeeperMaster(clusterDbId, shardDbId)).thenReturn(keeperMaster);
		when(currentMetaManager.getClusterMeta(clusterDbId)).thenReturn(new ClusterMeta().setActiveDc(getDc()));
		when(dcMetaCache.isCurrentDcPrimary(clusterDbId, shardDbId)).thenReturn(true);
		when(dcMetaCache.isCurrentDcBackUp(clusterDbId)).thenReturn(false);
		when(dcMetaCache.isCurrentShardParentCluster(clusterDbId, shardDbId)).thenReturn(true);
		when(dcMetaCache.getShardRedises(clusterDbId, shardDbId)).thenReturn(Collections.singletonList(redis));
	}
	
	@Test
	public void testDifferentShardExecute() throws Exception{

		Long clusterDbId1 = clusterDbId + 1;
		Long shardDbId1 = shardDbId + 1;

		List<KeeperMeta> newKeepers = Lists.newArrayList(MetaCloneFacade.INSTANCE.clone(keepers.get(1)).setActive(true));
		when(currentMetaManager.getSurviveKeepers(clusterDbId1, shardDbId1)).thenReturn(newKeepers);
		when(currentMetaManager.getKeeperMaster(clusterDbId1, shardDbId1)).thenReturn(keeperMaster);
		when(currentMetaManager.getClusterMeta(clusterDbId1)).thenReturn(new ClusterMeta().setActiveDc(getDc()));
		when(dcMetaCache.isCurrentDcPrimary(clusterDbId1, shardDbId1)).thenReturn(true);
		when(dcMetaCache.isCurrentDcBackUp(clusterDbId)).thenReturn(false);

		when(dcMetaCache.isCurrentDcBackUp(clusterDbId1)).thenReturn(false);
		when(dcMetaCache.isCurrentShardParentCluster(clusterDbId1, shardDbId1)).thenReturn(true);


		handler.keeperActiveElected(clusterDbId, shardDbId, null);
		handler.keeperActiveElected(clusterDbId1, shardDbId1, null);

		sleep(setStateTimeMilli/2);
		Assert.assertEquals(2, calledCount.get());
	}
	
	@Test
	public void testSameShardExecute() throws Exception{
		
		handler.keeperActiveElected(clusterDbId, shardDbId, null);
		handler.keeperActiveElected(clusterDbId, shardDbId, null);
		waitConditionUntilTimeOut(() -> calledCount.get() >= 1);
		sleep(setStateTimeMilli*3/2);
		Assert.assertEquals(2, calledCount.get());
	}

	@Test
	public void testBackupDcAdjust() throws Exception {
		setStateTimeMilli = 1;
		when(dcMetaCache.isCurrentDcBackUp(clusterDbId)).thenReturn(true);

		handler.keeperActiveElected(clusterDbId, shardDbId, keepers.get(0));
		waitConditionUntilTimeOut(() -> calledCount.get() >= 1);
		waitConditionUntilTimeOut(() -> 1 <= redisCall.get());
	}

	@Test
	public void testDownstreamDcAdjust() throws Exception {
		setStateTimeMilli = 1;
		when(dcMetaCache.isCurrentShardParentCluster(clusterDbId, shardDbId)).thenReturn(false);
		when(dcMetaCache.getCurrentDc()).thenReturn("currentdc");
		when(dcMetaCache.getShardAppliers(clusterDbId, shardDbId)).thenReturn(appliers);
		when(currentMetaManager.getGtidSet(anyLong(), anyString())).thenReturn(new GtidSet(""));
		when(currentMetaManager.getSrcSids(anyLong(), anyLong())).thenReturn("111");

		handler.keeperActiveElected(clusterDbId, shardDbId, keepers.get(0));
		waitConditionUntilTimeOut(() -> calledCount.get() >= 1);
		waitConditionUntilTimeOut(() -> 1 <= applierCall.get());
	}

	@Test
	public void testBackupDcBecomePrimaryDc() throws Exception {
		setStateTimeMilli = 1;
		when(dcMetaCache.isCurrentDcBackUp(clusterDbId)).thenReturn(true, false);

		handler.keeperActiveElected(clusterDbId, shardDbId, keepers.get(0));
		waitConditionUntilTimeOut(() -> calledCount.get() >= 1);
		sleep(1000);
		Assert.assertEquals(0, redisCall.get());
	}

	@After
	public void adterDefaultKeeperStateChangeHandlerTest(){
		
	}

	@Test
	public void testTfsShardUsesTfsSwitchJob() throws Exception {
		setStateTimeMilli = 50;
		keeperCallOrder.clear();
		KeeperMeta oldTfs = keepers.get(0).setKeeperContainerId(2L).setActive(false);
		KeeperMeta newBm = MetaCloneFacade.INSTANCE.clone(keepers.get(1)).setKeeperContainerId(1L).setActive(true);
		List<KeeperMeta> surviveKeepers = Lists.newArrayList(oldTfs, newBm);

		when(currentMetaManager.getSurviveKeepers(clusterDbId, shardDbId)).thenReturn(surviveKeepers);
		when(currentMetaManager.getPreviousActiveKeeper(clusterDbId, shardDbId)).thenReturn(oldTfs);
		when(dcMetaCache.getKeeperContainer(any(KeeperMeta.class))).thenAnswer(invocation -> {
			KeeperMeta keeperMeta = invocation.getArgument(0);
			KeeperContainerMeta keeperContainerMeta = new KeeperContainerMeta();
			keeperContainerMeta.setDiskType(keeperMeta.getKeeperContainerId() == 2L ? "tfs" : "DEFAULT");
			return keeperContainerMeta;
		});

		handler.keeperActiveElected(clusterDbId, shardDbId, newBm);
		waitConditionUntilTimeOut(() -> keeperCallOrder.size() >= 2);
		Assert.assertEquals(2, keeperCallOrder.size());
		Assert.assertEquals(newBm.getPort() + ":ACTIVE", keeperCallOrder.get(0));
		Assert.assertEquals(oldTfs.getPort() + ":BACKUP", keeperCallOrder.get(1));
		Assert.assertFalse(keeperCallOrder.stream().anyMatch(entry -> entry.endsWith(":PREPARE")));
	}

	private void recordKeeperSetState(int port, String request) {
		String state = parseKeeperSetState(request);
		if (state != null) {
			keeperCallOrder.add(port + ":" + state);
		}
	}

	private String parseKeeperSetState(String request) {
		if (request == null) {
			return null;
		}
		if (request.contains("setstate PREPARE")) {
			return "PREPARE";
		}
		if (request.contains("setstate ACTIVE")) {
			return "ACTIVE";
		}
		if (request.contains("setstate BACKUP")) {
			return "BACKUP";
		}
		return null;
	}
}
