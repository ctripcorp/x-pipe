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

	@Test
	public void testPrepareWatchAndPubsubDefaultsWhenQConfigAbsent() {
		KeeperConfig keeperConfig = new DefaultKeeperConfig();
		Assert.assertFalse(keeperConfig.isPrepareStoreWatchEnabled());
		Assert.assertFalse(keeperConfig.isPubsubParseEnabled());
		Assert.assertEquals(KeeperConfig.DEFAULT_PREPARE_WATCH_REOPEN_INTERVAL_MILLI,
				keeperConfig.getPrepareWatchReopenIntervalMilli());
		Assert.assertEquals(KeeperConfig.DEFAULT_PREPARE_WATCH_CLOSE_HOLD_MILLI,
				keeperConfig.getPrepareWatchCloseHoldMilli());
	}

	@Test
	public void testTestKeeperConfigPrepareWatchAndPubsubMatchProductionDefaults() {
		KeeperConfig keeperConfig = new TestKeeperConfig();
		Assert.assertFalse(keeperConfig.isPrepareStoreWatchEnabled());
		Assert.assertFalse(keeperConfig.isPubsubParseEnabled());
		Assert.assertEquals(KeeperConfig.DEFAULT_PREPARE_WATCH_REOPEN_INTERVAL_MILLI,
				keeperConfig.getPrepareWatchReopenIntervalMilli());
		Assert.assertEquals(KeeperConfig.DEFAULT_PREPARE_WATCH_CLOSE_HOLD_MILLI,
				keeperConfig.getPrepareWatchCloseHoldMilli());
	}

	@Test
	public void testCommandOffsetNotifyThresholdDefaults() {
		String bytesKey = DefaultKeeperConfig.KEY_COMMAND_OFFSET_NOTIFY_BYTES_THRESHOLD;
		String timeKey = DefaultKeeperConfig.KEY_COMMAND_OFFSET_NOTIFY_TIME_MILLI_THRESHOLD;
		String oldBytes = System.getProperty(bytesKey);
		String oldTime = System.getProperty(timeKey);
		try {
			System.clearProperty(bytesKey);
			System.clearProperty(timeKey);
			KeeperConfig keeperConfig = new DefaultKeeperConfig();
			Assert.assertEquals(4096, keeperConfig.getCommandOffsetNotifyBytesThreshold());
			Assert.assertEquals(1, keeperConfig.getCommandOffsetNotifyTimeMilliThreshold());

			KeeperConfig testConfig = new TestKeeperConfig();
			Assert.assertEquals(KeeperConfig.DEFAULT_COMMAND_OFFSET_NOTIFY_BYTES_THRESHOLD,
					testConfig.getCommandOffsetNotifyBytesThreshold());
			Assert.assertEquals(KeeperConfig.DEFAULT_COMMAND_OFFSET_NOTIFY_TIME_MILLI_THRESHOLD,
					testConfig.getCommandOffsetNotifyTimeMilliThreshold());
		} finally {
			restoreProperty(bytesKey, oldBytes);
			restoreProperty(timeKey, oldTime);
		}
	}

	@Test
	public void testCommandOffsetNotifyThresholdOverride() {
		String bytesKey = DefaultKeeperConfig.KEY_COMMAND_OFFSET_NOTIFY_BYTES_THRESHOLD;
		String timeKey = DefaultKeeperConfig.KEY_COMMAND_OFFSET_NOTIFY_TIME_MILLI_THRESHOLD;
		String oldBytes = System.getProperty(bytesKey);
		String oldTime = System.getProperty(timeKey);
		try {
			System.setProperty(bytesKey, "8192");
			System.setProperty(timeKey, "5");
			KeeperConfig keeperConfig = new DefaultKeeperConfig();
			Assert.assertEquals(8192, keeperConfig.getCommandOffsetNotifyBytesThreshold());
			Assert.assertEquals(5, keeperConfig.getCommandOffsetNotifyTimeMilliThreshold());

			TestKeeperConfig testConfig = new TestKeeperConfig()
					.setCommandOffsetNotifyBytesThreshold(100)
					.setCommandOffsetNotifyTimeMilliThreshold(7);
			Assert.assertEquals(100, testConfig.getCommandOffsetNotifyBytesThreshold());
			Assert.assertEquals(7, testConfig.getCommandOffsetNotifyTimeMilliThreshold());
		} finally {
			restoreProperty(bytesKey, oldBytes);
			restoreProperty(timeKey, oldTime);
		}
	}

	private static void restoreProperty(String key, String oldValue) {
		if (oldValue == null) {
			System.clearProperty(key);
		} else {
			System.setProperty(key, oldValue);
		}
	}

	@Test
	public void testCommandOffsetNotifyCoalescingConfig() {
		String key = DefaultKeeperConfig.KEY_COMMAND_OFFSET_NOTIFY_COALESCING_ENABLED;
		String oldValue = System.getProperty(key);
		try {
			System.setProperty(key, "false");
			KeeperConfig keeperConfig = new DefaultKeeperConfig();
			Assert.assertFalse(keeperConfig.isCommandOffsetNotifyCoalescingEnabled());
		} finally {
			if (oldValue == null) {
				System.clearProperty(key);
			} else {
				System.setProperty(key, oldValue);
			}
		}
	}
}
