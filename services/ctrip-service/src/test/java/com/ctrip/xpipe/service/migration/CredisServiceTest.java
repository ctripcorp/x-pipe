package com.ctrip.xpipe.service.migration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.metric.HostPort;
import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.AbstractServiceTest;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.api.migration.OuterClientService.MigrationPublishResult;

/**
 * @author shyin
 *
 * Dec 22, 2016
 */
public class CredisServiceTest extends AbstractServiceTest {

	private CredisService outerClientService = (CredisService) OuterClientService.DEFAULT;

	@Test(expected =  IllegalStateException.class)
	public void testGet() throws Exception {

		CredisService.GetInstanceResult result = outerClientService.getInstance(new HostPort("127.0.0.1", 6379));
		logger.info("{}", result);

		result = outerClientService.getInstance(new HostPort("127.0.0.1", 63799));
		logger.info("{}", result);

		logger.info("{}", outerClientService.isInstanceUp(new HostPort("127.0.0.1", 6379)));

		logger.info("{}", outerClientService.isInstanceUp(new HostPort("10.2.24.216", 6379)));

		outerClientService.isInstanceUp(new HostPort("127.0.0.1", 63799));

	}


	@Test
	public void testMarkStatus() throws Exception {

		outerClientService.markInstanceDown(new HostPort("127.0.0.1", 6379));

		sleep(600000);

	}
	
	@Test
	public void testCredisMigrationPublishService() throws Exception {
		

		Assert.assertTrue(outerClientService instanceof CredisService);
		
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
		
		Assert.assertEquals("SHAJQ", ((CredisService) OuterClientService.DEFAULT).convertDcName("ntgxh"));
		Assert.assertEquals("SHAJQ", ((CredisService) OuterClientService.DEFAULT).convertDcName("NTGXH"));
		Assert.assertEquals("SHAOY", ((CredisService) OuterClientService.DEFAULT).convertDcName("fat"));
		Assert.assertEquals("SHAOY", ((CredisService) OuterClientService.DEFAULT).convertDcName("FAT"));
		Assert.assertEquals("dc", ((CredisService) OuterClientService.DEFAULT).convertDcName("dc"));
	}
}
