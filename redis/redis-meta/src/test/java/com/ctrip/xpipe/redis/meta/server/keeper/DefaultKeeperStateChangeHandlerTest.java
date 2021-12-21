package com.ctrip.xpipe.redis.meta.server.keeper;

import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

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
@RunWith(MockitoJUnitRunner.class)
public class DefaultKeeperStateChangeHandlerTest extends AbstractMetaServerTest{
	
	private DefaultKeeperStateChangeHandler handler;
	
	@Mock
	private CurrentMetaManager currentMetaManager;
	
	@Mock
	private DcMetaCache dcMetaCache;
	
	private String clusterId, shardId;
	private Long clusterDbId, shardDbId;
	
	private List<KeeperMeta> keepers;

	private RedisMeta redis;
	
	private Pair<String, Integer> keeperMaster;
	
	private int setStateTimeMilli = 1500;
	
	private AtomicInteger calledCount = new AtomicInteger();
	private AtomicInteger redisCall = new AtomicInteger();

	@Before
	public void beforeDefaultKeeperStateChangeHandlerTest() throws Exception{
		
		handler = new DefaultKeeperStateChangeHandler();
		handler.setClientPool(getXpipeNettyClientKeyedObjectPool());
		handler.setcurrentMetaManager(currentMetaManager);
		handler.setDcMetaCache(dcMetaCache);
		handler.setScheduled(scheduled);
		handler.setExecutors(executors);

		clusterId = getClusterId();
		shardId = getShardId();
		clusterDbId = getClusterDbId();
		shardDbId = getShardDbId();
		
		keepers = createRandomKeepers(2);
		redis = newRandomFakeRedisMeta("localhost", randomPort());
		
		keeperMaster = new Pair<>("localhost", randomPort());
		
		LifecycleHelper.initializeIfPossible(handler);
		LifecycleHelper.startIfPossible(handler);
		add(handler);

		startServer(keepers.get(0).getPort(), new Function<String, String>() {
			@Override
			public String apply(String s) {
				logger.info("callCount:{}, msg:{}", calledCount.get(), s);
				calledCount.incrementAndGet();
				sleep(setStateTimeMilli);
				return "+OK\r\n";
			}
		});

		startServer(keepers.get(1).getPort(), new Callable<String>() {

			@Override
			public String call() throws Exception {
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
		when(dcMetaCache.isCurrentDcPrimary(clusterDbId, shardDbId)).thenReturn(true);
		when(dcMetaCache.getShardRedises(clusterDbId, shardDbId)).thenReturn(Collections.singletonList(redis));
	}
	
	@Test
	public void testDifferentShardExecute() throws Exception{

		Long clusterDbId1 = clusterDbId + 1;
		Long shardDbId1 = shardDbId + 1;
		
		when(currentMetaManager.getSurviveKeepers(clusterDbId1, shardDbId1)).thenReturn(keepers);
		when(currentMetaManager.getKeeperMaster(clusterDbId1, shardDbId1)).thenReturn(keeperMaster);
		when(dcMetaCache.isCurrentDcPrimary(clusterDbId1, shardDbId1)).thenReturn(true);


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
		when(dcMetaCache.isCurrentDcPrimary(clusterDbId, shardDbId)).thenReturn(false);

		handler.keeperActiveElected(clusterDbId, shardDbId, keepers.get(0));
		waitConditionUntilTimeOut(() -> calledCount.get() >= 1);
		waitConditionUntilTimeOut(() -> 1 <= redisCall.get());
	}

	@Test
	public void testBackupDcBecomePrimaryDc() throws Exception {
		setStateTimeMilli = 1;
		when(dcMetaCache.isCurrentDcPrimary(clusterDbId, shardDbId)).thenReturn(false, true);

		handler.keeperActiveElected(clusterDbId, shardDbId, keepers.get(0));
		waitConditionUntilTimeOut(() -> calledCount.get() >= 1);
		sleep(1000);
		Assert.assertEquals(0, redisCall.get());
	}

	@After
	public void adterDefaultKeeperStateChangeHandlerTest(){
		
	}
}
