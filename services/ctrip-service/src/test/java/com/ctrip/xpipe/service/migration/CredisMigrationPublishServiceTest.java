package com.ctrip.xpipe.service.migration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.AbstractServiceTest;
import com.ctrip.xpipe.api.migration.MigrationPublishService;
import com.ctrip.xpipe.api.migration.MigrationPublishService.MigrationPublishResult;

/**
 * @author shyin
 *
 * Dec 22, 2016
 */
public class CredisMigrationPublishServiceTest extends AbstractServiceTest {
	
	@Test
	public void testCredisMigrationPublishService() throws IOException {
		
		MigrationPublishService publishService = MigrationPublishService.DEFAULT;
		
		Assert.assertTrue(publishService instanceof CredisMigrationPublishService);
		
		List<InetSocketAddress> newMasters = new LinkedList<>();
		
//		newMasters.add(new InetSocketAddress("10.2.58.242", 6379));
//		newMasters.add(new InetSocketAddress("10.2.58.243", 6389));
//		MigrationPublishResult result = publishService.doMigrationPublish("cluster_shyin", "SHAJQ", newMasters);

		newMasters.add(new InetSocketAddress("10.3.2.23", 6379));
		newMasters.add(new InetSocketAddress("10.3.2.23", 6389));

		MigrationPublishResult result = publishService.doMigrationPublish("cluster_shyin", "SHAOY", newMasters);
		
		logger.info("[testCredisMigrationPublishService]{}", result);		
	}
	
	@Test
	public void testConvertDcName() {
		
		Assert.assertEquals("SHAJQ", ((CredisMigrationPublishService)MigrationPublishService.DEFAULT).convertDcName("ntgxh"));
		Assert.assertEquals("SHAJQ", ((CredisMigrationPublishService)MigrationPublishService.DEFAULT).convertDcName("NTGXH"));
		Assert.assertEquals("SHAOY", ((CredisMigrationPublishService)MigrationPublishService.DEFAULT).convertDcName("fat"));
		Assert.assertEquals("SHAOY", ((CredisMigrationPublishService)MigrationPublishService.DEFAULT).convertDcName("FAT"));
		Assert.assertEquals("dc", ((CredisMigrationPublishService)MigrationPublishService.DEFAULT).convertDcName("dc"));
	}
}
