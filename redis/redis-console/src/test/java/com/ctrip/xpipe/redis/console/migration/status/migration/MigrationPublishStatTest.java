package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.migration.AbstractOuterClientService;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.web.client.ResourceAccessException;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;

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

		when(migrationCluster.getScheduled()).thenReturn(scheduled);
		when(migrationCluster.getMigrationExecutor()).thenReturn(executors);
		when(migrationCluster.getRedisService()).thenReturn(redisService);
		
	}

	@Test
	public void publishFailWithNetworkProblemTest() {

		MigrationPublishState stat = new MigrationPublishState(migrationCluster);
		stat.setPublishService(new AbstractOuterClientService() {
			@Override
			public DcMeta getOutClientDcMeta(String dc) throws Exception {
				return null;
			}

			@Override
			public MigrationPublishResult doMigrationPublish(String clusterName, String shardName, String primaryDcName,
					InetSocketAddress newMaster) {
				throw new ResourceAccessException("test");
			}

			@Override
			public MigrationPublishResult doMigrationPublish(String clusterName, String primaryDcName,
					List<InetSocketAddress> newMasters) {
				throw new ResourceAccessException("test");
			}
		});
		stat.getStateActionState().tryAction();
		verify(migrationCluster).updateStat(isA(MigrationPublishFailState.class));
	}
	
	@Test
	public void publishFailWithReturnFail() {

		MigrationPublishState stat = new MigrationPublishState(migrationCluster);
		stat.setPublishService(new AbstractOuterClientService() {
			@Override
			public DcMeta getOutClientDcMeta(String dc) throws Exception {
				return null;
			}

			@Override
			public MigrationPublishResult doMigrationPublish(String clusterName, String shardName, String primaryDcName,
					InetSocketAddress newMaster) {
				MigrationPublishResult res = new MigrationPublishResult();
				res.setSuccess(false);
				return res;
			}

			@Override
			public MigrationPublishResult doMigrationPublish(String clusterName, String primaryDcName,
					List<InetSocketAddress> newMasters) {
				MigrationPublishResult res = new MigrationPublishResult();
				res.setSuccess(false);
				return res;
			}
		});
		
		stat.getStateActionState().tryAction();
		verify(migrationCluster).updateStat(isA(MigrationPublishFailState.class));
	}
}
