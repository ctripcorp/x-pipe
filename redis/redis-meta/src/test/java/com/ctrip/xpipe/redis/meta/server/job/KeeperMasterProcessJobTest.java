package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.keeper.elect.KeeperRoleAssigner;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.simpleserver.AbstractIoActionFactory;
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
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
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

	@Test
	public void testCorrectWithRolesSetsPrepare() throws Exception {
		List<String> callOrder = Collections.synchronizedList(new ArrayList<>());
		when(dcMetaCache.getKeeperContainer(any(KeeperMeta.class))).thenAnswer(invocation -> {
			KeeperMeta keeperMeta = invocation.getArgument(0);
			KeeperContainerMeta containerMeta = new KeeperContainerMeta();
			containerMeta.setId(keeperMeta.getKeeperContainerId());
			containerMeta.setDiskType(keeperMeta.getKeeperContainerId() >= 2L ? "tfs-1" : "DEFAULT");
			return containerMeta;
		});

		Server master = getMasterServer("127.0.0.1", 6379);
		int masterPort = master.getPort();
		when(dcMetaCache.isCurrentDcPrimary(anyLong(), anyLong())).thenReturn(true);
		when(dcMetaCache.getShardRedises(clusterId, shardId))
				.thenReturn(Collections.singletonList(new RedisMeta().setIp("127.0.0.1").setPort(masterPort)));

		KeeperMeta bm = keeper(7121, 1L, 1, true);
		KeeperMeta highTfs = keeper(7122, 2L, 5, false);
		KeeperMeta lowTfs = keeper(7123, 3L, 1, false);
		List<KeeperMeta> shardKeepers = new LinkedList<>();
		shardKeepers.add(bm);
		shardKeepers.add(highTfs);
		shardKeepers.add(lowTfs);

		Map<KeeperMeta, KeeperState> roles = KeeperRoleAssigner.assignRoles(bm, shardKeepers, dcMetaCache);
		Assert.assertEquals(KeeperState.PREPARE, roles.get(lowTfs));

		startKeeperSetStateServer(bm.getPort(), callOrder);
		startKeeperSetStateServer(highTfs.getPort(), callOrder);
		startKeeperSetStateServer(lowTfs.getPort(), callOrder);

		job = new KeeperMasterProcessJob(clusterId, shardId, shardKeepers, new RouteMeta(), dcMetaCache,
				new Pair<>("127.0.0.1", masterPort), getXpipeNettyClientKeyedObjectPool(), scheduled, executors, roles);
		job.execute().get(5000, TimeUnit.MILLISECONDS);

		Assert.assertTrue(job.future().isSuccess());
		Assert.assertTrue(callOrder.contains(lowTfs.getPort() + ":PREPARE"));
		Assert.assertFalse(callOrder.contains(lowTfs.getPort() + ":BACKUP"));
	}

	private void startKeeperSetStateServer(int port, List<String> callOrder) throws Exception {
		startServer(port, new AbstractIoActionFactory() {
			@Override
			protected byte[] getToWrite(Object readResult) {
				String request = (String) readResult;
				if (request != null) {
					if (request.contains("setstate PREPARE")) {
						callOrder.add(port + ":PREPARE");
					} else if (request.contains("setstate ACTIVE")) {
						callOrder.add(port + ":ACTIVE");
					} else if (request.contains("setstate BACKUP")) {
						callOrder.add(port + ":BACKUP");
					}
				}
				return "+OK\r\n".getBytes();
			}
		});
	}

	private KeeperMeta keeper(int port, long keeperContainerId, Integer priority, boolean active) {
		KeeperMeta keeperMeta = new KeeperMeta();
		keeperMeta.setIp("127.0.0.1");
		keeperMeta.setPort(port);
		keeperMeta.setKeeperContainerId(keeperContainerId);
		keeperMeta.setPriority(priority);
		keeperMeta.setActive(active);
		return keeperMeta;
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
