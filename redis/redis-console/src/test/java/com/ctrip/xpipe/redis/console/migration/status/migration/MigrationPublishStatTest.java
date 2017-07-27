package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.api.migration.OuterClientException;
import com.ctrip.xpipe.metric.HostPort;
import com.ctrip.xpipe.redis.console.migration.status.MigrationState;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.web.client.ResourceAccessException;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationPublishState;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;


import static org.mockito.Mockito.*;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author shyin
 *
 *         Dec 22, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class MigrationPublishStatTest extends AbstractConsoleTest {
	@Mock
	private MigrationCluster migrationCluster;
	@Mock
	private ClusterService clusterService;
	@Mock
	private RedisService redisService;
	@Mock
	private MigrationService migrationService;

	@Before
	public void setUp() throws ResourceNotFoundException {
		when(migrationCluster.getCurrentCluster()).thenReturn((new ClusterTbl().setClusterName("test-cluster")));
		when(migrationCluster.getMigrationCluster()).thenReturn((new MigrationClusterTbl()).setDestinationDcId(1));

		Map<Long, ShardTbl> shards = new HashMap<>();
		shards.put(1L, ((new ShardTbl()).setShardName("test-shard1")));
		shards.put(2L, ((new ShardTbl()).setShardName("test-shard2")));
		Map<Long, DcTbl> dcs = new HashMap<>();
		dcs.put(1L, (new DcTbl()).setDcName("test-dc"));
		when(migrationCluster.getClusterShards()).thenReturn(shards);
		when(migrationCluster.getClusterDcs()).thenReturn(dcs);

		when(migrationCluster.getClusterService()).thenReturn(clusterService);
		when(migrationCluster.getRedisService()).thenReturn(redisService);
		when(migrationCluster.getMigrationService()).thenReturn(migrationService);
		when(redisService.findAllByDcClusterShard("test-dc", "test-cluster", "test-shard1"))
				.thenReturn(Arrays.asList((new RedisTbl()).setMaster(true).setRedisIp("0.0.0.0").setRedisPort(0)));
		when(redisService.findAllByDcClusterShard("test-dc", "test-cluster", "test-shard2"))
		.thenReturn(Arrays.asList((new RedisTbl()).setMaster(true).setRedisIp("0.0.0.1").setRedisPort(0)));
		
	}

	@Test
	public void migrationPublishStatActionTest() {
		MigrationState stat = new MigrationPublishState(migrationCluster);
		MigrationState spy = spy(stat);

		spy.action();
		verify(spy, times(1)).nextAfterSuccess();
		verify(spy, times(0)).nextAfterFail();
	}
	
	@Test
	public void publishFailWithNetworkProblemTest() {
		MigrationPublishState stat = new MigrationPublishState(migrationCluster);
		MigrationPublishState spy = spy(stat);
		doReturn(new OuterClientService() {

			
			@Override
			public int getOrder() {
				return 0;
			}
			
			@Override
			public MigrationPublishResult doMigrationPublish(String clusterName, String shardName, String primaryDcName,
					InetSocketAddress newMaster) {
				throw new ResourceAccessException("test");
			}


			@Override
			public void markInstanceUp(HostPort hostPort) throws OuterClientException {

			}

			@Override
			public boolean isInstanceUp(HostPort hostPort) throws OuterClientException {
				return false;
			}

			@Override
			public void markInstanceDown(HostPort hostPort) throws OuterClientException {

			}

			@Override
			public MigrationPublishResult doMigrationPublish(String clusterName, String primaryDcName,
					List<InetSocketAddress> newMasters) {
				throw new ResourceAccessException("test");
			}
		}).when(spy).getMigrationPublishService();
		
		spy.action();
		verify(spy, times(0)).nextAfterSuccess();
		verify(spy, times(1)).nextAfterFail();
	}
	
	@Test
	public void publishFailWithReturnFail() {
		MigrationPublishState stat = new MigrationPublishState(migrationCluster);
		MigrationPublishState spy = spy(stat);
		doReturn(new OuterClientService() {
			
			@Override
			public int getOrder() {
				return 0;
			}
			
			@Override
			public MigrationPublishResult doMigrationPublish(String clusterName, String shardName, String primaryDcName,
					InetSocketAddress newMaster) {
				MigrationPublishResult res = new MigrationPublishResult();
				res.setSuccess(false);
				return res;
			}

			@Override
			public void markInstanceUp(HostPort hostPort) throws OuterClientException {

			}

			@Override
			public boolean isInstanceUp(HostPort hostPort) throws OuterClientException {
				return true;
			}

			@Override
			public void markInstanceDown(HostPort hostPort) throws OuterClientException {

			}

			@Override
			public MigrationPublishResult doMigrationPublish(String clusterName, String primaryDcName,
					List<InetSocketAddress> newMasters) {
				MigrationPublishResult res = new MigrationPublishResult();
				res.setSuccess(false);
				return res;
			}
		}).when(spy).getMigrationPublishService();
		
		spy.action();
		verify(spy, times(0)).nextAfterSuccess();
		verify(spy, times(1)).nextAfterFail();
	}
}
