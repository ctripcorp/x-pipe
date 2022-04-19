package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.store.CommandsGuarantee;
import com.ctrip.xpipe.redis.core.store.CommandsListener;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.store.cmd.OffsetCommandReaderWriterFactory;
import com.ctrip.xpipe.redis.keeper.store.cmd.OffsetReplicationProgress;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;

import static com.ctrip.xpipe.redis.keeper.store.DefaultCommandStore.DEFAULT_COMMAND_READER_FLYING_THRESHOLD;

/**
 * @author wenchao.meng
 *
 *         Sep 12, 2016
 */
public class DefaultCommandStoreTest extends AbstractRedisKeeperTest {

	private DefaultCommandStore commandStore;

	private File commandTemplate;

	private int maxFileSize = 1 << 10;

	private int minWritten = (1 << 13);

	private OffsetCommandReaderWriterFactory commandReaderWriterFactory = new OffsetCommandReaderWriterFactory();

	@Before
	public void beforeDefaultCommandStoreTest() throws IOException {

		String testDir = getTestFileDir();
		commandTemplate = new File(testDir, getTestName());
		commandStore = new DefaultCommandStore(commandTemplate, maxFileSize, commandReaderWriterFactory, createkeeperMonitor());
	}

	@Test
	public void testDynamicConfig() throws IOException {

		final int initDataKeep = 20;
		final AtomicInteger dataKeep = new AtomicInteger(initDataKeep);
		int gcAfterCreateMilli = 60000;
		File commandTemplate = new File(getTestFileDir(), getTestName());

		commandStore = new DefaultCommandStore(commandTemplate, maxFileSize, () -> 3600, gcAfterCreateMilli, () -> dataKeep.get(), DEFAULT_COMMAND_READER_FLYING_THRESHOLD,
				commandReaderWriterFactory, createkeeperMonitor()){
			@Override
			public long totalLength() {
				return initDataKeep * maxFileSize;
			}
		};

		Assert.assertFalse(commandStore.canDeleteCmdFile(maxFileSize * 10, 0, maxFileSize, new Date().getTime() - 600000));

		dataKeep.set(19);
		Assert.assertFalse(commandStore.canDeleteCmdFile(maxFileSize * 10, 0, maxFileSize, new Date().getTime() - 600000));

		dataKeep.set(18);
		Assert.assertTrue(commandStore.canDeleteCmdFile(maxFileSize * 10, 0, maxFileSize, new Date().getTime() - 600000));

	}

	@Test
	public void testInterruptClose() throws InterruptedException{
		
		Thread thread = new Thread(new AbstractExceptionLogTask() {
			
			@Override
			protected void doRun() throws Exception {
				
				while(true){
					commandStore.totalLength();
					sleep(10);
				}
			}
		});
		
		thread.start();
		
		thread.interrupt();
		
		thread.join(100);
		//should not fail
		commandStore.totalLength();
	}
	
	@Test
	public void testLengthEqual() throws InterruptedException{
		
		final int runTimes = 1000; 
		final AtomicLong realLength = new AtomicLong();
		final AtomicReference<Boolean> result = new AtomicReference<Boolean>(true);
		
		final Semaphore read = new Semaphore(0);
		final Semaphore write = new Semaphore(1);
		final AtomicBoolean finished = new AtomicBoolean(false);
		final CountDownLatch latch = new CountDownLatch(2);
		
		executors.execute(new AbstractExceptionLogTask() {
			
			@Override
			protected void doRun() throws Exception {
				
				try{
					for(int i=0;i<runTimes;i++){
						write.acquire();
						int randomLength = randomInt(0, 1 << 8);
						commandStore.appendCommands(Unpooled.wrappedBuffer(randomString(randomLength).getBytes()));
						realLength.addAndGet(randomLength);
						read.release();
					}
				}finally{
					finished.set(true);
					read.release();
					latch.countDown();
				}
				
			}
		});

		executors.execute(new AbstractExceptionLogTask() {
			
			@Override
			protected void doRun() throws Exception {
				
				try{
					while(!finished.get()){
						
						read.acquire();
						long len = commandStore.totalLength();
						if(len != realLength.get()){
							result.set(false);
						}
						write.release();
					}
				}finally{
					latch.countDown();
				}
			}
		});

		latch.await();
		Assert.assertTrue(result.get());
	}
	
	
	
	@Test
	public void testGetAsSoonAsMessageWritten() throws IOException, InterruptedException {

		final StringBuilder sb = new StringBuilder();
		final Semaphore semaphore = new Semaphore(0);

		executors.execute(new AbstractExceptionLogTask() {

			@Override
			protected void doRun() throws Exception {
				commandStore.addCommandsListener(new OffsetReplicationProgress(0), new CommandsListener() {

					@Override
					public ChannelFuture onCommand(ReferenceFileRegion referenceFileRegion) {

						sb.append(readFileChannelInfoMessageAsString(referenceFileRegion));
						semaphore.release();
						return null;
					}

					@Override
					public boolean isOpen() {
						return true;
					}

					@Override
					public void beforeCommand() {

					}

					@Override
					public Long processedOffset() {
						return null;
					}
				});
			}
		});

		StringBuilder expected = new StringBuilder();
		for (int i = 0; i < (1 << 10); i++) {

			byte random = (byte) randomInt('a', 'z');
			semaphore.drainPermits();
			expected.append((char) random);
			commandStore.appendCommands(Unpooled.wrappedBuffer(new byte[] { random }));

			Assert.assertTrue(semaphore.tryAcquire(1000, TimeUnit.MILLISECONDS));
			logger.debug("{}", sb);
			Assert.assertEquals(expected.toString(), sb.toString());
		}

	}

	@Test
	public void testConcurrentRotateGetFileLength() throws IOException, InterruptedException, ExecutionException {

		final AtomicReference<DefaultCommandStore> commandStore = new AtomicReference<>();
		final int appendCount = 10;

		try {
			String testDir = getTestFileDir();
			File commandTemplate = new File(testDir, getTestName());
			commandStore.set(new DefaultCommandStore(commandTemplate, 1, commandReaderWriterFactory, createkeeperMonitor()));
			final AtomicBoolean appendResult = new AtomicBoolean(false);
			final SettableFuture<Void> future = SettableFuture.create();

			executors.execute(new Runnable() {

				@Override
				public void run() {

					try {
						for (int i = 0; i < appendCount; i++) {
							commandStore.get().appendCommands(Unpooled.wrappedBuffer(randomString(10).getBytes()));
						}
					} catch (IOException e) {
						logger.error("[run]", e);
					} finally {
						appendResult.set(true);
					}
				}
			});

			executors.execute(new Runnable() {

				@Override
				public void run() {

					while (!appendResult.get()) {
						try {
							commandStore.get().totalLength();
						} catch (Exception e) {
							future.setException(e);
						}
					}
					future.set(null);
				}
			});

			future.get();
		} finally {
			if (commandStore.get() != null) {
				commandStore.get().close();
			}
		}
	}

	@Test
	public void testReadNotFromZero() throws IOException, InterruptedException {

		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < 20; i++) {
			int length = randomInt(100, 500);
			String random = randomString(length);
			sb.append(random);
			commandStore.appendCommands(Unpooled.wrappedBuffer(random.getBytes()));
		}

		int totalLength = sb.length();

		for (int i = 0; i < 20; i++) {

			long offset = randomInt(0, totalLength);
			int expected = (int) (totalLength - offset);
			String result = readCommandStoreTilNoMessage(offset, commandStore, expected);
			Assert.assertEquals(sb.substring((int) offset), result);
		}

	}

	private String readCommandStoreTilNoMessage(final DefaultCommandStore commandStore, int expectedLength)
			throws InterruptedException {

		return readCommandStoreTilNoMessage(0, commandStore, expectedLength);
	}

	private String readCommandStoreTilNoMessage(final long offset, final DefaultCommandStore commandStore,
			int expectedLength) throws InterruptedException {

		final StringBuilder result = new StringBuilder();
		final Semaphore semaphore = new Semaphore(-expectedLength + 1);

		new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					commandStore.addCommandsListener(new OffsetReplicationProgress(offset), new CommandsListener() {

						@Override
						public ChannelFuture onCommand(ReferenceFileRegion referenceFileRegion) {

							logger.debug("[onCommand]{}", referenceFileRegion);
							result.append(readFileChannelInfoMessageAsString(referenceFileRegion));
							semaphore.release((int) referenceFileRegion.count());
							return null;
						}

						@Override
						public boolean isOpen() {
							return true;
						}

						@Override
						public void beforeCommand() {

						}

						@Override
						public Long processedOffset() {
							return null;
						}
					});
				} catch (IOException e) {
					logger.error("[run]", e);
				}
			}
		}).start();

		semaphore.tryAcquire(10, TimeUnit.SECONDS);
		return result.toString();
	}

	@Test
	public void testReadWrite() throws IOException, InterruptedException {

		StringBuilder sb = new StringBuilder();
		AtomicInteger totalWritten = new AtomicInteger();

		while (true) {
			int length = randomInt(100, 500);
			String random = randomString(length);
			sb.append(random);
			totalWritten.addAndGet(length);
			commandStore.appendCommands(Unpooled.wrappedBuffer(random.getBytes()));
			if (totalWritten.get() >= minWritten) {
				logger.info("[testReadWrite][totalWritten]{}, {}", totalWritten, minWritten);
				break;
			}
		}

		String result = readCommandStoreTilNoMessage(commandStore, sb.length());
		logger.info("[testReadWrite]{}, {}", sb.length(), result.length());
		Assert.assertTrue(sb.toString().equals(result));
	}
	
	@Test
	public void testBeginRead() throws IOException{
		
		int testCount = 10;
		long total = commandStore.totalLength();
				
		Assert.assertEquals(0, total);
		for(int i=0;i < testCount;i++){
			
			commandStore.appendCommands(Unpooled.wrappedBuffer(randomString(maxFileSize).getBytes()));
			total += maxFileSize;
			commandStore.beginRead(new OffsetReplicationProgress(total));
		}
		
	}


	@Test
	public void testGcOldCmdFile() throws Exception {
		AtomicInteger maxSecondsKeepCmdFile = new AtomicInteger(60);

		commandStore = new DefaultCommandStore(commandTemplate, 100, maxSecondsKeepCmdFile::get, 0,
				() -> 20, DEFAULT_COMMAND_READER_FLYING_THRESHOLD, commandReaderWriterFactory, createkeeperMonitor());
		appendCommandsToStore(10, 100);

		commandStore.gc();
		Assert.assertEquals(0, commandStore.lowestAvailableOffset());

		maxSecondsKeepCmdFile.set(1);
		sleep(1000);
		appendCommandsToStore(1, 10);
		commandStore.gc();
		Assert.assertEquals(1000, commandStore.lowestAvailableOffset());
	}

	@Test
	public void testGc() throws Exception {
		int fileNumToKeep = 2;

		commandStore = new DefaultCommandStore(commandTemplate, 100, () -> 3600, 0,
				() -> fileNumToKeep, DEFAULT_COMMAND_READER_FLYING_THRESHOLD, commandReaderWriterFactory, createkeeperMonitor());
		appendCommandsToStore(3, 100);

		commandStore.gc();
		Assert.assertEquals(0, commandStore.lowestAvailableOffset());

		appendCommandsToStore(1, 100);
		commandStore.gc();
		Assert.assertEquals(100, commandStore.lowestAvailableOffset());
	}

	@Test
	public void testRetainCommands() throws Exception {
		int fileNumToKeep = 2;

		commandStore = new DefaultCommandStore(commandTemplate, 100, () -> 3600, 0,
				() -> fileNumToKeep, DEFAULT_COMMAND_READER_FLYING_THRESHOLD, commandReaderWriterFactory, createkeeperMonitor());
		appendCommandsToStore(3, 100);

		commandStore.retainCommands(buildCommandGuarantee(0, 0, 100000, () -> true, () -> 0));
		appendCommandsToStore(1, 100);
		commandStore.gc();
		Assert.assertEquals(0, commandStore.lowestAvailableOffset());
	}

	@Test
	public void testRetainCommandsButTimeout() throws Exception {
		int fileNumToKeep = 2;

		commandStore = new DefaultCommandStore(commandTemplate, 100, () -> 3600, 0,
				() -> fileNumToKeep, DEFAULT_COMMAND_READER_FLYING_THRESHOLD, commandReaderWriterFactory, createkeeperMonitor());
		appendCommandsToStore(3, 100);

		commandStore.retainCommands(buildCommandGuarantee(0, 0, 1, () -> true, () -> 0));
		sleep(10);
		appendCommandsToStore(1, 100);
		commandStore.gc();
		Assert.assertEquals(100, commandStore.lowestAvailableOffset());
	}

	@Test
	public void testRetainCommandsButListenerClosed() throws Exception {
		int fileNumToKeep = 2;

		commandStore = new DefaultCommandStore(commandTemplate, 100, () -> 3600, 0,
				() -> fileNumToKeep, DEFAULT_COMMAND_READER_FLYING_THRESHOLD, commandReaderWriterFactory, createkeeperMonitor());
		appendCommandsToStore(3, 100);

		commandStore.retainCommands(buildCommandGuarantee(0, 0, 100000, () -> false, () -> 0));
		sleep(10);
		appendCommandsToStore(1, 100);
		commandStore.gc();
		Assert.assertEquals(100, commandStore.lowestAvailableOffset());
	}

	@Test
	public void testRetainCommandsAndFinish() throws Exception {
		int fileNumToKeep = 2;

		commandStore = new DefaultCommandStore(commandTemplate, 100, () -> 3600, 0,
				() -> fileNumToKeep, DEFAULT_COMMAND_READER_FLYING_THRESHOLD, commandReaderWriterFactory, createkeeperMonitor());
		appendCommandsToStore(3, 100);

		commandStore.retainCommands(buildCommandGuarantee(0, 0, 100000, () -> true, () -> 10));
		sleep(10);
		appendCommandsToStore(1, 100);
		commandStore.gc();
		Assert.assertEquals(100, commandStore.lowestAvailableOffset());
	}

	private CommandsGuarantee buildCommandGuarantee(long beginOffset, long offset, long timeoutMilli, BooleanSupplier isOpen, LongSupplier processedOffset) {
		return new DefaultCommandsGuarantee(new CommandsListener() {
			@Override
			public boolean isOpen() {
				return isOpen.getAsBoolean();
			}

			@Override
			public ChannelFuture onCommand(ReferenceFileRegion referenceFileRegion) {
				return null;
			}

			@Override
			public void beforeCommand() {

			}

			@Override
			public Long processedOffset() {
				return processedOffset.getAsLong();
			}
		}, beginOffset, offset, timeoutMilli);
	}

	private void appendCommandsToStore(int batch, int batchSize) {
		IntStream.range(0, batch).forEach(i -> {
			try {
				commandStore.appendCommands(Unpooled.wrappedBuffer(randomString(batchSize).getBytes()));
			} catch (Exception e) {
				logger.info("[appendCommandsToStore][fail]", e);
			}
		});
	}

	@After
	public void afterDefaultCommandStoreTest() throws IOException {
		commandStore.close();
	}

}
