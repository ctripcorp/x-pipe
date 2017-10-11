package com.ctrip.xpipe.redis.keeper.config;


import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 *
 * Aug 18, 2016
 */
public class DefaultKeeperConfigTest extends AbstractRedisKeeperTest{
	
	@Test
	public void testKeeperConfig(){
		
		KeeperConfig keeperConfig = new DefaultKeeperConfig();
		logger.info("[keeperConfig]{}, {}", Codec.DEFAULT.encode(keeperConfig), keeperConfig);
	}
	
	@Test
	public void testReplicationStoreMaxCommandsToTransferBeforeCreateRdb(){
		
		KeeperConfig keeperConfig = new DefaultKeeperConfig();
		long value = 10L * (1<<30);
		
		System.setProperty(DefaultKeeperConfig.KEY_REPLICATION_STORE_MAX_COMMANDS_TO_TRANSFER_BEFORE_CREATE_RDB, String.valueOf(value));

		logger.info("[testReplicationStoreMaxCommandsToTransferBeforeCreateRdb]{}", value);
		Assert.assertEquals(value, keeperConfig.getReplicationStoreMaxCommandsToTransferBeforeCreateRdb());
	}

}
