package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.command.DefaultRetryCommandFactory;
import com.ctrip.xpipe.command.RetryCommandFactory;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
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
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.mockito.Mockito.*;

/**
 * @author wenchao.meng
 *
 * Jan 4, 2017
 */
@RunWith(MockitoJUnitRunner.class)
public class KeeperMasterCheckJobTest extends AbstractMetaServerTest{

	private KeeperMasterCheckJob job;

	@Mock
	private DcMetaCache dcMetaCache;
	private Long clusterId = 1L;
	private Long shardId = 1L;
	private XpipeMeta xpipeMeta;
	private RetryCommandFactory retryCommandFactory;
	private ExecutorService executors;


	@Before
	public void beforeKeeperStateChangeJobTest() throws Exception{
		job = new KeeperMasterCheckJob(clusterId, shardId, dcMetaCache,new Pair<>("localhost", randomPort()), getXpipeNettyClientKeyedObjectPool(), executors, scheduled);
		xpipeMeta = loadXpipeMeta(getXpipeMetaConfigFile());
		retryCommandFactory = DefaultRetryCommandFactory.retryNTimes(scheduled, 1, 10);
	}

	@Test
	public void testIsRedis() {
		when(dcMetaCache.getShardRedises(clusterId, shardId)).thenReturn(xpipeMeta.getDcs().get("jq").getClusters().get("cluster1").getShards().get("shard1").getRedises());
		Assert.assertTrue(job.isRedis(clusterId, shardId, "127.0.0.1", 6379));
		Assert.assertFalse(job.isRedis(clusterId, shardId, "127.0.0.1", 6000));
	}


	@Test
	public void testKeeperMasterCheck_BackupDc() throws Exception {
		Server master = getMasterServer("127.0.0.1", 6379);
		int port = master.getPort();
		job = new KeeperMasterCheckJob(clusterId, shardId, dcMetaCache,new Pair<>("127.0.0.1", port), getXpipeNettyClientKeyedObjectPool(), executors, scheduled);
		job = spy(job);
		when(dcMetaCache.isCurrentDcPrimary(anyLong(), anyLong())).thenReturn(false);
		job.execute();
		verify(job, times(0)).isRedis(clusterId, shardId, "127.0.0.1", port);
		Assert.assertTrue(job.future().isSuccess());
	}

	@Test
	public void testKeeperMasterCheck_NotRedis() throws Exception {
		Server master = getMasterServer("127.0.0.1", 6379);
		int port = master.getPort();
		System.out.println("master port:" + port);
		job = new KeeperMasterCheckJob(clusterId, shardId, dcMetaCache, new Pair<>("127.0.0.1", port), getXpipeNettyClientKeyedObjectPool(), executors, scheduled);
		job = spy(job);
		when(job.isRedis(anyLong(), anyLong(), anyString(), anyInt())).thenReturn(false);
		when(dcMetaCache.isCurrentDcPrimary(anyLong(), anyLong())).thenReturn(true);
		job.execute();
		verify(job, times(1)).isRedis(clusterId, shardId, "127.0.0.1", port);
		Assert.assertFalse(job.future().isSuccess());
		Assert.assertEquals(job.future().cause().getMessage(), String.format("keeperMaster:127.0.0.1:%d, error:not redis", port));
	}

	@Test
	public void testKeeperMasterCheck_NotMaster() throws Exception {
		when(dcMetaCache.isCurrentDcPrimary(anyLong(), anyLong())).thenReturn(true);

		Server slave = getSlaveServer("127.0.0.1", 5374, MASTER_STATE.REDIS_REPL_CONNECTED);
		int port = slave.getPort();

		job = new KeeperMasterCheckJob(clusterId, shardId, dcMetaCache, new Pair<>("127.0.0.1", port), getXpipeNettyClientKeyedObjectPool(), executors, scheduled);
		job = spy(job);
		when(job.isRedis(anyLong(), anyLong(), anyString(), anyInt())).thenReturn(true);
		job.execute();
		verify(job, times(1)).isRedis(clusterId, shardId, "127.0.0.1", port);
		waitConditionUntilTimeOut(()-> job.future().isDone());
		Assert.assertFalse(job.future().isSuccess());
		Assert.assertEquals(job.future().cause().getMessage(), String.format("keeperMaster:127.0.0.1:%d, error:not master", port));
	}

	@Test
	public void testKeeperMasterCheck_OneMaster() throws Exception {
		Server master = getMasterServer("127.0.0.1", 6379);
		int port = master.getPort();

		when(dcMetaCache.isCurrentDcPrimary(anyLong(), anyLong())).thenReturn(true);
		when(dcMetaCache.getShardRedises(clusterId, shardId)).thenReturn(Collections.singletonList(new RedisMeta().setIp("127.0.0.1").setPort(port)));

		job = new KeeperMasterCheckJob(clusterId, shardId, dcMetaCache, new Pair<>("127.0.0.1", port), getXpipeNettyClientKeyedObjectPool(), executors, scheduled);
		job = spy(job);
		when(job.isRedis(anyLong(), anyLong(), anyString(), anyInt())).thenReturn(true);
		job.execute();
		verify(job, times(1)).isRedis(clusterId, shardId, "127.0.0.1", port);
		waitConditionUntilTimeOut(()-> job.future().isDone());
		Assert.assertTrue(job.future().isSuccess());
	}

	@Test
	public void testKeeperMasterCheck_TwoMaster() throws Exception {
		Server master = getMasterServer("127.0.0.1", 6379);
		Server slave = getSlaveServer("127.0.0.1", 5374, MASTER_STATE.REDIS_REPL_CONNECTED);
		Server master2 = getMasterServer("127.0.0.1", 6378);

		List<RedisMeta> redisMetaList = new ArrayList<>();
		redisMetaList.add(new RedisMeta().setIp("127.0.0.1").setPort(master.getPort()));
		redisMetaList.add(new RedisMeta().setIp("127.0.0.1").setPort(master2.getPort()));
		redisMetaList.add(new RedisMeta().setIp("127.0.0.1").setPort(slave.getPort()));

		when(dcMetaCache.isCurrentDcPrimary(anyLong(), anyLong())).thenReturn(true);
		when(dcMetaCache.getShardRedises(clusterId, shardId)).thenReturn(redisMetaList);

		job = new KeeperMasterCheckJob(clusterId, shardId, dcMetaCache, new Pair<>("127.0.0.1", master.getPort()), getXpipeNettyClientKeyedObjectPool(), executors, scheduled);
		job = spy(job);
		job.execute();
		waitConditionUntilTimeOut(()-> job.future().isDone());
		Assert.assertFalse(job.future().isSuccess());
		Assert.assertEquals(job.future().cause().getMessage(), String.format("keeperMaster:127.0.0.1:%d, error:two master", master.getPort()));
	}

	@Test
	public void testKeeperMasterCheck_Timeout() throws Exception {
		Server master = getMasterServer("127.0.0.1", 6379);
		Server master2 = getMasterServer("127.0.0.1", 6378);

		List<RedisMeta> redisMetaList = new ArrayList<>();
		redisMetaList.add(new RedisMeta().setIp("127.0.0.1").setPort(master.getPort()));
		redisMetaList.add(new RedisMeta().setIp("127.0.0.1").setPort(master2.getPort()));
		redisMetaList.add(new RedisMeta().setIp("127.0.0.1").setPort(1111));

		when(dcMetaCache.isCurrentDcPrimary(anyLong(), anyLong())).thenReturn(true);
		when(dcMetaCache.getShardRedises(clusterId, shardId)).thenReturn(redisMetaList);

		job = new KeeperMasterCheckJob(clusterId, shardId, dcMetaCache, new Pair<>("127.0.0.1", master.getPort()), getXpipeNettyClientKeyedObjectPool(), executors, scheduled);
		job = spy(job);
		job.execute();
		waitConditionUntilTimeOut(()-> job.future().isDone());
		Assert.assertFalse(job.future().isSuccess());
		Assert.assertEquals(job.future().cause().getMessage(), String.format("keeperMaster:127.0.0.1:%d, error:two master", master.getPort()));
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
