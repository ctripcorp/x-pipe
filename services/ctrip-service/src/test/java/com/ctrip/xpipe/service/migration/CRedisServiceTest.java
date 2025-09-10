package com.ctrip.xpipe.service.migration;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.api.migration.OuterClientService.MigrationPublishResult;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.service.AbstractServiceTest;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;

/**
 * @author shyin
 *
 * Dec 22, 2016
 */
public class CRedisServiceTest extends AbstractServiceTest {

	private CRedisService outerClientService = (CRedisService) OuterClientService.DEFAULT;

	@Test
	public void testGetClusterInfo() throws Exception {

		JsonCodec jsonCodec = new JsonCodec(true);

		logger.info("{}", outerClientService.getClusterInfo("Members.OfflineLogin"));
		logger.info("{}", jsonCodec.encode(outerClientService.getClusterInfo("cluster_shyin")));
		logger.info("{}", outerClientService.getClusterInfo("Members.OfflineLogin"));
		logger.info("{}", jsonCodec.encode(outerClientService.getClusterInfo("cluster_shyin_1")));

	}

	@Test
	public void testExcludeIdcs() throws Exception {

		logger.info("{}", outerClientService.excludeIdcs("test_aula", new String[]{"shaxy"}));
		logger.info("{}", outerClientService.excludeIdcs("SingleDCHasMultiDcInstancesCluster", new String[]{}));
	}

	@Test(expected =  IllegalStateException.class)
	public void testGet() throws Exception {

		CRedisService.GetInstanceResult result = outerClientService.getInstance(newHostPort("127.0.0.1", 6379));
		logger.info("{}", result);

		result = outerClientService.getInstance(newHostPort("127.0.0.1", 63799));
		logger.info("{}", result);

		logger.info("{}", outerClientService.isInstanceUp(newHostPort("127.0.0.1", 6379)));

		logger.info("{}", outerClientService.isInstanceUp(newHostPort("10.2.24.216", 6379)));

		outerClientService.isInstanceUp(newHostPort("127.0.0.1", 63799));

	}

	private ClusterShardHostPort newHostPort(String host, int port) {
		return new ClusterShardHostPort("cluster_shyin", "shard2", new HostPort(host, port));
	}

	
	@Test
	public void testCredisMigrationPublishService() throws Exception {
		

		Assert.assertTrue(outerClientService instanceof CRedisService);
		
		List<InetSocketAddress> newMasters = new LinkedList<>();
		
//		newMasters.add(new InetSocketAddress("10.2.58.242", 6379));
//		newMasters.add(new InetSocketAddress("10.2.58.243", 6389));
//		MigrationPublishResult result = publishService.doMigrationPublish("cluster_shyin", "SHAJQ", newMasters);

		newMasters.add(new InetSocketAddress("10.3.2.23", 6379));
		newMasters.add(new InetSocketAddress("10.3.2.23", 6389));

		MigrationPublishResult result = outerClientService.doMigrationPublish("cluster_shyin", "SHAOY", newMasters);
		
		logger.info("[testCredisMigrationPublishService]{}", result);		
	}
	
	@Test
	public void testConvertDcName() {
		
		Assert.assertEquals("SHAJQ", ((CRedisService) OuterClientService.DEFAULT).convertDcName("ntgxh"));
		Assert.assertEquals("SHAJQ", ((CRedisService) OuterClientService.DEFAULT).convertDcName("NTGXH"));
		Assert.assertEquals("SHAOY", ((CRedisService) OuterClientService.DEFAULT).convertDcName("fat"));
		Assert.assertEquals("SHAOY", ((CRedisService) OuterClientService.DEFAULT).convertDcName("FAT"));
		Assert.assertEquals("dc", ((CRedisService) OuterClientService.DEFAULT).convertDcName("dc"));
	}
}
