package com.ctrip.xpipe.service.migration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.AbstractServiceTest;
import com.ctrip.xpipe.api.migration.MigrationPublishService;

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
		
		System.out.println(publishService.doMigrationPublish("cluster", "dc", Arrays.asList(InetSocketAddress.createUnresolved("127.0.0.1", 6379))));
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
