package com.ctrip.xpipe.redis.keeper.config;


import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

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

	/**
	 * leverage 'debug' mode to run when testing, see if there were more than 1 Long.parseLong() operates per 10 sec*/
	@Ignore
	@Test
	public void testGetLongProperty() {
		KeeperConfig keeperConfig = new DefaultKeeperConfig();
		long value = 10L * (1<<30);

		System.setProperty(DefaultKeeperConfig.KEY_REPLICATION_STORE_MAX_COMMANDS_TO_TRANSFER_BEFORE_CREATE_RDB, String.valueOf(value));

		for(int i = 0; i < 100; i++) {
			Assert.assertEquals(value, keeperConfig.getReplicationStoreMaxCommandsToTransferBeforeCreateRdb());
		}
		long preValue = value;
		value = 10L * (1<<10);

		System.setProperty(DefaultKeeperConfig.KEY_REPLICATION_STORE_MAX_COMMANDS_TO_TRANSFER_BEFORE_CREATE_RDB, String.valueOf(value));
		((DefaultKeeperConfig) keeperConfig)
				.onChange(DefaultKeeperConfig.KEY_REPLICATION_STORE_MAX_COMMANDS_TO_TRANSFER_BEFORE_CREATE_RDB,
						String.valueOf(preValue), String.valueOf(value));
		for(int i = 0; i < 100; i++) {
			Assert.assertEquals(value, keeperConfig.getReplicationStoreMaxCommandsToTransferBeforeCreateRdb());
		}
	}

	/**
	 * leverage 'debug' mode to run when testing, see if there were more than 1 Long.parseLong() operates per 10 sec*/
	@Ignore
	@Test
	public void testGetLongPropertyMultiThread() {
		KeeperConfig keeperConfig = new DefaultKeeperConfig();

		AtomicLong value = new AtomicLong(randomInt());
		System.setProperty(DefaultKeeperConfig.KEY_REPLICATION_STORE_MAX_COMMANDS_TO_TRANSFER_BEFORE_CREATE_RDB, String.valueOf(value.get()));

		for(int i = 0; i < 100; i++)
		executors.execute(new AbstractExceptionLogTask() {
			@Override
			protected void doRun() throws Exception {
				while(!Thread.currentThread().isInterrupted()) {
					Assert.assertEquals(value.get(), keeperConfig.getReplicationStoreMaxCommandsToTransferBeforeCreateRdb());
					sleep(100);
				}
			}
		});
		new Thread(){
			@Override
			public void run() {
				while(!Thread.currentThread().isInterrupted()) {
					try {
						sleep(1000 * 10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					((DefaultKeeperConfig) keeperConfig)
							.onChange(DefaultKeeperConfig.KEY_REPLICATION_STORE_MAX_COMMANDS_TO_TRANSFER_BEFORE_CREATE_RDB,
									String.valueOf(value.getAndSet(randomInt())), String.valueOf(value));
					System.setProperty(DefaultKeeperConfig.KEY_REPLICATION_STORE_MAX_COMMANDS_TO_TRANSFER_BEFORE_CREATE_RDB, String.valueOf(value.get()));
				}
			}
		}.start();
		sleep(1000 * 60);
	}

	@Test
	public void testNullPointExceptionWhenNoValueSet() {
		KeeperConfig keeperConfig = new DefaultKeeperConfig();
		keeperConfig.getReplicationStoreMaxCommandsToTransferBeforeCreateRdb();
	}
}
