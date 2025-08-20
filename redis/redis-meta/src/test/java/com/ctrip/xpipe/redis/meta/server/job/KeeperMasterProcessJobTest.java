package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class KeeperMasterProcessJobTest extends AbstractMetaServerTest{

	KeeperMasterProcessJob job;
	private List<KeeperMeta> keepers;

	@Mock
	private DcMetaCache dcMetaCache;
	private Long clusterId = 1L;
	private Long shardId = 1L;
	private ExecutorService executors;


	@Before
	public void beforeKeeperMasterProcessJobTest() throws Exception{
		keepers = new LinkedList<>();
		job = new KeeperMasterProcessJob(clusterId, shardId, keepers, new RouteMeta(), dcMetaCache, new Pair<>("localhost", randomPort()), getXpipeNettyClientKeyedObjectPool(), scheduled, executors);
	}


	@Test
	public void testNotRedis() throws Exception {
		Server master = getMasterServer("127.0.0.1", 6379);
		int port = master.getPort();
		job = new KeeperMasterProcessJob(clusterId, shardId, keepers, new RouteMeta(), dcMetaCache, new Pair<>("127.0.0.1", port), getXpipeNettyClientKeyedObjectPool(), scheduled, executors);
		job = spy(job);
		when(dcMetaCache.isCurrentDcPrimary(anyLong(), anyLong())).thenReturn(true);
		when(dcMetaCache.getShardRedises(clusterId, shardId)).thenReturn(Collections.singletonList(new RedisMeta().setIp("127.0.0.1").setPort(port+1)));
		job.execute();
		waitConditionUntilTimeOut(() -> job.future().isDone());
		Assert.assertFalse(job.future().isSuccess());
		Assert.assertTrue(job.future().cause().getMessage().contains(String.format("keeperMaster:127.0.0.1:%d, error:not redis", port)));
	}

	@Test
	public void testNotMaster() throws Exception {
		when(dcMetaCache.isCurrentDcPrimary(anyLong(), anyLong())).thenReturn(true);

		Server slave = getSlaveServer("127.0.0.1", 5374, MASTER_STATE.REDIS_REPL_CONNECTED);
		int port = slave.getPort();
		job = new KeeperMasterProcessJob(clusterId, shardId, keepers, new RouteMeta(), dcMetaCache, new Pair<>("127.0.0.1", port), getXpipeNettyClientKeyedObjectPool(), scheduled, executors);
		job = spy(job);
		when(dcMetaCache.isCurrentDcPrimary(anyLong(), anyLong())).thenReturn(true);
		when(dcMetaCache.getShardRedises(clusterId, shardId)).thenReturn(Collections.singletonList(new RedisMeta().setIp("127.0.0.1").setPort(port)));
		job.execute();
		waitConditionUntilTimeOut(()-> job.future().isDone());
		Assert.assertFalse(job.future().isSuccess());
		Assert.assertTrue(job.future().cause().getMessage().contains(String.format("keeperMaster:127.0.0.1:%d, error:not master", port)));
	}

	@Test
	public void testOneMaster() throws Exception {
		Server master = getMasterServer("127.0.0.1", 6379);
		int port = master.getPort();

		when(dcMetaCache.isCurrentDcPrimary(anyLong(), anyLong())).thenReturn(true);
		when(dcMetaCache.getShardRedises(clusterId, shardId)).thenReturn(Collections.singletonList(new RedisMeta().setIp("127.0.0.1").setPort(port)));
		job = new KeeperMasterProcessJob(clusterId, shardId, keepers, new RouteMeta(), dcMetaCache, new Pair<>("127.0.0.1", port), getXpipeNettyClientKeyedObjectPool(), scheduled, executors);
		job = spy(job);
		job.execute();
		waitConditionUntilTimeOut(()-> job.future().isDone());
		Assert.assertFalse(job.future().isSuccess());
		Assert.assertTrue( job.future().cause().getMessage().contains("can not find active keeper:[]"));
	}

	@Test
	public void testTwoMaster() throws Exception {
		Server master = getMasterServer("127.0.0.1", 6379);
		Server slave = getSlaveServer("127.0.0.1", 5374, MASTER_STATE.REDIS_REPL_CONNECTED);
		Server master2 = getMasterServer("127.0.0.1", 6378);

		List<RedisMeta> redisMetaList = new ArrayList<>();
		redisMetaList.add(new RedisMeta().setIp("127.0.0.1").setPort(master.getPort()));
		redisMetaList.add(new RedisMeta().setIp("127.0.0.1").setPort(master2.getPort()));
		redisMetaList.add(new RedisMeta().setIp("127.0.0.1").setPort(slave.getPort()));

		when(dcMetaCache.isCurrentDcPrimary(anyLong(), anyLong())).thenReturn(true);
		when(dcMetaCache.getShardRedises(clusterId, shardId)).thenReturn(redisMetaList);
		job = new KeeperMasterProcessJob(clusterId, shardId, keepers, new RouteMeta(), dcMetaCache, new Pair<>("127.0.0.1", master.getPort()), getXpipeNettyClientKeyedObjectPool(), scheduled, executors);

		job = spy(job);
		job.execute();
		waitConditionUntilTimeOut(()-> job.future().isDone());
		Assert.assertFalse(job.future().isSuccess());
		Assert.assertTrue(job.future().cause().getMessage().contains(String.format("keeperMaster:127.0.0.1:%d, error:multi master", master.getPort())));
	}

	private Server getMasterServer(String ip, int port) throws Exception {
		return startServer("*3\r\n"
				+ "$6\r\nmaster\r\n"
				+ ":43\r\n"
				+ "*3\r\n"
				+ "$9\r\n" + ip + "\r\n"
				+ "$4\r\n"+ port + "\r\n"
				+ "$1\r\n0\r\n");
	}

	private Server getSlaveServer(String ip, int port, MASTER_STATE masterState) throws Exception {
        return startServer("*5\r\n"
                + "$5\r\nslave\r\n"
                + "$9\r\n" + ip +"\r\n"
                + ":" + port +"\r\n"
                + "$" +masterState.getDesc().length()+ "\r\n" + masterState.getDesc()+ "\r\n"
                + ":477\r\n");
	}

}
