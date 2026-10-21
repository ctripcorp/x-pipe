package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserFactory;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.GeneralRedisOpParser;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;
import com.ctrip.xpipe.redis.keeper.store.ck.CKStore;
import com.ctrip.xpipe.redis.core.store.ratelimit.ReplDelayConfig;
import com.ctrip.xpipe.redis.keeper.store.cmd.OffsetCommandReader;
import com.ctrip.xpipe.redis.keeper.store.cmd.OffsetCommandReaderWriterFactory;
import com.ctrip.xpipe.redis.keeper.store.cmd.CoalescingOffsetNotifier;
import com.ctrip.xpipe.utils.OffsetNotifier;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * @author wenchao.meng
 *
 *         Sep 12, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultCommandStoreTest extends AbstractRedisKeeperTest {

	private DefaultCommandStore commandStore;

	private File commandTemplate;

	private int maxFileSize = 1 << 10;

	private int minWritten = (1 << 13);

	private OffsetCommandReaderWriterFactory commandReaderWriterFactory = new OffsetCommandReaderWriterFactory();

	private RedisOpParser opParser;

	@Mock
	private GtidCmdFilter gtidCmdFilter;

	@Mock
	private CKStore ckStore;


	private AtomicInteger failed = new AtomicInteger(0);


	@Before
	public void beforeDefaultCommandStoreTest() {
		commandTemplate = new File(getTestFileDir(), getTestName() + "_");
		RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
		RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
		opParser = new GeneralRedisOpParser(redisOpParserManager);
	}

	@After
	public void afterDefaultCommandStoreTest() throws IOException {
		closeCommandStore();
	}

	/** Default store: field maxFileSize, buildIndex=true. Closes previous writer first (single-writer). */
	private void initCommandStore() throws IOException {
		initCommandStore(maxFileSize);
	}

	private void initCommandStore(int fileSize) throws IOException {
		openCommandStore(createDefaultCommandStore(commandTemplate, fileSize, commandReaderWriterFactory,
				createkeeperMonitor(), opParser, gtidCmdFilter));
	}

	/** Take ownership of a constructed store: close previous writer, then initialize. */
	private void openCommandStore(DefaultCommandStore store) throws IOException {
		closeCommandStore();
		commandStore = store;
		commandStore.initialize();
	}

	private void closeCommandStore() throws IOException {
		if (commandStore != null) {
			commandStore.close();
			commandStore = null;
		}
	}

	@Test
	public void testDynamicConfig() throws Exception {

		final int initDataKeep = 20;
		final AtomicInteger dataKeep = new AtomicInteger(initDataKeep);
		int gcAfterCreateMilli = 60000;

		openCommandStore(new DefaultCommandStore(null, getKeeperConfig(), commandTemplate, maxFileSize, () -> false, () -> 3600, gcAfterCreateMilli, () -> dataKeep.get(), DEFAULT_COMMAND_READER_FLYING_THRESHOLD, () -> true,
				commandReaderWriterFactory, createkeeperMonitor(), opParser, gtidCmdFilter, true, 0L, asyncFileSystem(), () -> AsyncCommandStore.DEFAULT_ASYNC_WRITE_MAX_BYTES, getReplId()){
			@Override
			public long totalLength() {
				return initDataKeep * maxFileSize;
			}
		});

		Assert.assertFalse(commandStore.canDeleteCmdFile(maxFileSize * 10, 0, maxFileSize, new Date().getTime() - 600000));

		dataKeep.set(19);
		Assert.assertFalse(commandStore.canDeleteCmdFile(maxFileSize * 10, 0, maxFileSize, new Date().getTime() - 600000));

		dataKeep.set(18);
		Assert.assertTrue(commandStore.canDeleteCmdFile(maxFileSize * 10, 0, maxFileSize, new Date().getTime() - 600000));

	}

	@Test
	public void testNotifyImmediatelyWhenCoalescingDisabled() throws Exception {
		openCommandStore(createDefaultCommandStore(null, getKeeperConfig(), commandTemplate, maxFileSize, () -> false, () -> 3600, 0, () -> 20,
				DEFAULT_COMMAND_READER_FLYING_THRESHOLD, () -> false, commandReaderWriterFactory, createkeeperMonitor(), opParser, gtidCmdFilter, true));
		ReflectionTestUtils.setField(commandStore, "buildIndex", false);

		commandStore.appendCommands(Unpooled.wrappedBuffer(new byte[] { 'a' }));
		Assert.assertTrue(commandStore.awaitCommandsOffset(0, 10));
	}

	@Test
	public void testInterruptClose() throws Exception {
		initCommandStore();

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
	public void testLengthEqual() throws Exception {
		initCommandStore();
		ReflectionTestUtils.setField(commandStore, "buildIndex", false);
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
	public void testGetAsSoonAsMessageWritten() throws Exception {

		initCommandStore();
		final StringBuilder sb = new StringBuilder();
		final Semaphore semaphore = new Semaphore(0);

		ReflectionTestUtils.setField(commandStore, "buildIndex", false);

		executors.execute(new AbstractExceptionLogTask() {

			@Override
			protected void doRun() throws Exception {
				commandStore.addCommandsListener(new OffsetReplicationProgress(0), new CommandsListener() {

					@Override
					public ChannelFuture onCommand(Object referenceFileRegion) {

						sb.append(readReferenceFileRegionAsString((ReferenceFileRegion) referenceFileRegion));
						semaphore.release();
						return null;
					}

					@Override
					public void onCommandEnd() {

					}

					@Override
					public boolean isOpen() {
						return true;
					}

					@Override
					public void beforeCommand() {

					}

					@Override
					public Long processedBacklogOffset() {
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

	/**
	 * Reproduce KeeperSingleDcSlaveof#testSlaveof stall: reader already on tail
	 * ({@code openedEnd=MAX}), then writer rolls and appends a new segment.
	 * Distinguishes A ({@code totalLength} stuck) vs B ({@code transferTo} returns 0).
	 */
	@Test
	public void testPreopenedTailReaderSeesDataAfterRotate() throws Exception {
		initCommandStore();
		ReflectionTestUtils.setField(commandStore, "buildIndex", false);

		byte[] oldSeg = new byte[2000];
		Arrays.fill(oldSeg, (byte) 'A');
		commandStore.appendCommands(Unpooled.wrappedBuffer(oldSeg));
		Assert.assertEquals(2000L, commandStore.totalLength());

		OffsetNotifier notifier = (OffsetNotifier) ReflectionTestUtils.getField(commandStore, "offsetNotifier");
		OffsetCommandReader reader = (OffsetCommandReader) commandReaderWriterFactory.createCmdReader(
				new OffsetReplicationProgress(0), commandStore, notifier, new ReplDelayConfig() {},
				DEFAULT_COMMAND_READER_FLYING_THRESHOLD);
		commandStore.addReader(reader);

		ByteArrayOutputStream drained = new ByteArrayOutputStream();
		WritableByteChannel drainCh = allBytesChannel(drained);
		long got = drainReader(reader, drainCh, 2000);
		Assert.assertEquals("reader should catch up to old tail before rotate", 2000L, got);

		AsyncSegmentFile readHandle = (AsyncSegmentFile) ReflectionTestUtils.getField(reader, "readAsyncSegmentFile");
		long openedStartBefore = (Long) ReflectionTestUtils.getField(readHandle, "openedSegmentStartOffset");
		long openedEndBefore = (Long) ReflectionTestUtils.getField(readHandle, "openedSegmentEndOffset");
		logger.info("[pre-rotate] reader opened=[{}, {}) totalLength={}",
				openedStartBefore, openedEndBefore, commandStore.totalLength());

		commandStore.rotateFileIfNecessary();

		AsyncFileSystem fs = commandStore.getAsyncFileSystem();
		AsyncSegmentFile writerHandle = commandStore.getAsyncSegmentFile();
		long writerStartAfterRotate = fs.getCurrentSegmentStartOffset(writerHandle);
		long writerSegSizeAfterRotate = fs.sizeOfSegment(writerHandle, writerStartAfterRotate).get(5, TimeUnit.SECONDS);
		logger.info("[post-rotate] writerStart={} writerSegSize={} totalLength={} list={}",
				writerStartAfterRotate, writerSegSizeAfterRotate, commandStore.totalLength(), fs.list(writerHandle));

		byte[] newTail = new byte[8000];
		Arrays.fill(newTail, (byte) 'B');
		commandStore.appendCommands(Unpooled.wrappedBuffer(newTail));

		long writerStart = fs.getCurrentSegmentStartOffset(writerHandle);
		long writerSegSize = fs.sizeOfSegment(writerHandle, writerStart).get(5, TimeUnit.SECONDS);
		long totalAfter = commandStore.totalLength();
		logger.info("[post-write] writerStart={} writerSegSize={} totalLength={} list={}",
				writerStart, writerSegSize, totalAfter, fs.list(writerHandle));

		long openedStartAfter = (Long) ReflectionTestUtils.getField(readHandle, "openedSegmentStartOffset");
		long openedEndAfter = (Long) ReflectionTestUtils.getField(readHandle, "openedSegmentEndOffset");
		logger.info("[post-write] reader opened=[{}, {})", openedStartAfter, openedEndAfter);

		boolean hypothesisA = totalAfter <= 2000;
		ReferenceFileRegion region = reader.read(50);
		long transferred = 0;
		if (region != null && region != ReferenceFileRegion.EOF) {
			transferred = region.transferTo(drainCh, 0);
			reader.flushed(region);
		}
		boolean hypothesisB = !hypothesisA && (region == null || transferred <= 0);

		logger.info("[A/B] totalAfter={} region={} regionCount={} transferred={} A={} B={}",
				totalAfter, region == null ? "null" : region.getClass().getSimpleName(),
				region == null || region == ReferenceFileRegion.EOF ? -1 : region.count(),
				transferred, hypothesisA, hypothesisB);

		try {
			if (hypothesisA) {
				Assert.fail("A: totalLength stuck at " + totalAfter + " after writing 8000 bytes past rotate");
			}
			if (hypothesisB) {
				Assert.fail("B: totalLength=" + totalAfter + " but transferTo=" + transferred
						+ " readerOpened=[" + openedStartAfter + "," + openedEndAfter + ")");
			}
			Assert.assertEquals(10000L, totalAfter);
			Assert.assertTrue("reader should receive new tail after rotate", transferred > 0);
		} finally {
			reader.close();
		}
	}

	private static WritableByteChannel allBytesChannel(ByteArrayOutputStream out) {
		return new WritableByteChannel() {
			private boolean open = true;

			@Override
			public int write(ByteBuffer src) {
				int n = src.remaining();
				byte[] buf = new byte[n];
				src.get(buf);
				out.write(buf, 0, n);
				return n;
			}

			@Override
			public boolean isOpen() {
				return open;
			}

			@Override
			public void close() {
				open = false;
			}
		};
	}

	private static long drainReader(OffsetCommandReader reader, WritableByteChannel ch, long expect)
			throws IOException {
		long got = 0;
		for (int i = 0; i < 64 && got < expect; i++) {
			ReferenceFileRegion region = reader.read(50);
			if (region == null || region == ReferenceFileRegion.EOF) {
				break;
			}
			got += region.transferTo(ch, 0);
			reader.flushed(region);
		}
		return got;
	}

	@Test
	public void testConcurrentRotateGetFileLength() throws Exception {

		final int appendCount = 10;
		initCommandStore(1);

		final DefaultCommandStore store = commandStore;
		final AtomicBoolean appendResult = new AtomicBoolean(false);
		final SettableFuture<Void> future = SettableFuture.create();

		executors.execute(new Runnable() {

			@Override
			public void run() {

				try {
					for (int i = 0; i < appendCount; i++) {
						store.appendCommands(Unpooled.wrappedBuffer(randomString(10).getBytes()));
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
						store.totalLength();
					} catch (Exception e) {
						future.setException(e);
					}
				}
				future.set(null);
			}
		});

		future.get();
	}

	@Test
	public void testReadNotFromZero() throws Exception {

		initCommandStore();
		StringBuilder sb = new StringBuilder();
		ReflectionTestUtils.setField(commandStore, "buildIndex", false);

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
						public ChannelFuture onCommand(Object referenceFileRegion) {

							logger.debug("[onCommand]{}", referenceFileRegion);
							ReferenceFileRegion region = (ReferenceFileRegion) referenceFileRegion;
							result.append(readReferenceFileRegionAsString(region));
							semaphore.release((int) region.count());
							return null;
						}

						@Override
						public void onCommandEnd() {

						}

						@Override
						public boolean isOpen() {
							return true;
						}

						@Override
						public void beforeCommand() {

						}

						@Override
						public Long processedBacklogOffset() {
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
	public void testReadWrite() throws Exception {

		initCommandStore();
		StringBuilder sb = new StringBuilder();
		AtomicInteger totalWritten = new AtomicInteger();

		ReflectionTestUtils.setField(commandStore, "buildIndex", false);

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
	public void testAsyncSegmentWriteRollAndRead() throws Exception {
		// maxFileSize=5, asyncWriteMaxBytes=2：CommandStore 仅在每次 appendCommands 写前 rotate，
		// 同一次 append 内不中途 roll。
		openCommandStore(createDefaultCommandStore(null, getKeeperConfig(), commandTemplate, 5, () -> false, () -> 3600, 0,
				() -> 20, DEFAULT_COMMAND_READER_FLYING_THRESHOLD, () -> false, commandReaderWriterFactory,
				createkeeperMonitor(), opParser, gtidCmdFilter, false, () -> 2));

		String first = "abcde";
		commandStore.appendCommands(Unpooled.wrappedBuffer(first.getBytes(Codec.defaultCharset)));
		Assert.assertEquals(first.length(), commandStore.totalLength());
		Assert.assertTrue(new File(commandTemplate.getParentFile(), commandTemplate.getName() + "0").isFile());
		Assert.assertFalse(new File(commandTemplate.getParentFile(), commandTemplate.getName() + "5").isFile());

		String second = "fg";
		commandStore.appendCommands(Unpooled.wrappedBuffer(second.getBytes(Codec.defaultCharset)));
		String expected = first + second;
		Assert.assertEquals(expected.length(), commandStore.totalLength());
		Assert.assertTrue(new File(commandTemplate.getParentFile(), commandTemplate.getName() + "0").isFile());
		Assert.assertTrue(new File(commandTemplate.getParentFile(), commandTemplate.getName() + "5").isFile());
		Assert.assertEquals(expected, readCommandStoreTilNoMessage(commandStore, expected.length()));
	}


	@Test
	public void testGcOldCmdFile() throws Exception {
		AtomicInteger maxSecondsKeepCmdFile = new AtomicInteger(60);

		openCommandStore(createDefaultCommandStore(null, getKeeperConfig(), commandTemplate, 100, () -> false, maxSecondsKeepCmdFile::get, 0,
				() -> 20, DEFAULT_COMMAND_READER_FLYING_THRESHOLD, () -> true, commandReaderWriterFactory, createkeeperMonitor(),
				opParser, gtidCmdFilter, true));
		appendCommandsToStore(10, 100);

		commandStore.gc();
		Assert.assertEquals(0, commandStore.lowestAvailableOffset());

		maxSecondsKeepCmdFile.set(1);
		sleep(1000);
		appendCommandsToStore(1, 10);
		commandStore.gc();
		Assert.assertEquals(0, commandStore.lowestAvailableOffset());
	}

	@Test
	public void testGc() throws Exception {
		int fileNumToKeep = 2;

		openCommandStore(createDefaultCommandStore(null, getKeeperConfig(), commandTemplate, 100, () -> false, () -> 3600, 0,
				() -> fileNumToKeep, DEFAULT_COMMAND_READER_FLYING_THRESHOLD, () -> true, commandReaderWriterFactory,
				createkeeperMonitor(), opParser, gtidCmdFilter, true));
		appendCommandsToStore(3, 100);

		commandStore.gc();
		Assert.assertEquals(0, commandStore.lowestAvailableOffset());

		appendCommandsToStore(1, 100);
		commandStore.gc();
		Assert.assertEquals(0, commandStore.lowestAvailableOffset());
	}

	@Test
	public void testRetainCommands() throws Exception {
		int fileNumToKeep = 2;

		openCommandStore(createDefaultCommandStore(null, getKeeperConfig(), commandTemplate, 100, () -> false, () -> 3600, 0,
				() -> fileNumToKeep, DEFAULT_COMMAND_READER_FLYING_THRESHOLD, () -> true, commandReaderWriterFactory,
				createkeeperMonitor(), opParser, gtidCmdFilter, true));
		appendCommandsToStore(3, 100);

		commandStore.retainCommands(buildCommandGuarantee(0, 100000, () -> true, () -> 0));
		appendCommandsToStore(1, 100);
		commandStore.gc();
		Assert.assertEquals(0, commandStore.lowestAvailableOffset());
	}

	@Test
	public void testRetainCommandsButTimeout() throws Exception {
		int fileNumToKeep = 2;

		openCommandStore(createDefaultCommandStore(null, getKeeperConfig(), commandTemplate, 100, () -> false, () -> 3600, 0,
				() -> fileNumToKeep, DEFAULT_COMMAND_READER_FLYING_THRESHOLD, () -> true,
				commandReaderWriterFactory, createkeeperMonitor(), opParser, gtidCmdFilter, true));
		appendCommandsToStore(3, 100);

		commandStore.retainCommands(buildCommandGuarantee(0, 1, () -> true, () -> 0));
		sleep(10);
		appendCommandsToStore(1, 100);
		commandStore.gc();
		Assert.assertEquals(0, commandStore.lowestAvailableOffset());
	}

	@Test
	public void testRetainCommandsButListenerClosed() throws Exception {
		int fileNumToKeep = 2;

		openCommandStore(createDefaultCommandStore(null, getKeeperConfig(), commandTemplate, 100, () -> false, () -> 3600, 0,
				() -> fileNumToKeep, DEFAULT_COMMAND_READER_FLYING_THRESHOLD, () -> true, commandReaderWriterFactory,
				createkeeperMonitor(), opParser, gtidCmdFilter, true));
		appendCommandsToStore(3, 100);

		commandStore.retainCommands(buildCommandGuarantee(0, 100000, () -> false, () -> 0));
		sleep(10);
		appendCommandsToStore(1, 100);
		commandStore.gc();
		Assert.assertEquals(0, commandStore.lowestAvailableOffset());
	}

	@Test
	public void testRetainCommandsAndFinish() throws Exception {
		int fileNumToKeep = 2;

		openCommandStore(createDefaultCommandStore(null, getKeeperConfig(), commandTemplate, 100, () -> false, () -> 3600, 0,
				() -> fileNumToKeep, DEFAULT_COMMAND_READER_FLYING_THRESHOLD, () -> true,
				commandReaderWriterFactory, createkeeperMonitor(), opParser, gtidCmdFilter, true));
		appendCommandsToStore(3, 100);

		commandStore.retainCommands(buildCommandGuarantee(0, 100000, () -> true, () -> 10));
		sleep(10);
		appendCommandsToStore(1, 100);
		commandStore.gc();
		Assert.assertEquals(0, commandStore.lowestAvailableOffset());
	}

	private CommandsGuarantee buildCommandGuarantee(long backlogOffset, long timeoutMilli, BooleanSupplier isOpen, LongSupplier processedOffset) {
		return new DefaultCommandsGuarantee(new CommandsListener() {
			@Override
			public boolean isOpen() {
				return isOpen.getAsBoolean();
			}

			@Override
			public ChannelFuture onCommand(Object cmd) {
				return null;
			}

			@Override
			public void onCommandEnd() {

			}

			@Override
			public void beforeCommand() {

			}

			@Override
			public Long processedBacklogOffset() {
				return processedOffset.getAsLong();
			}
		}, backlogOffset, () -> (int)timeoutMilli);
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

	private String readReferenceFileRegionAsString(ReferenceFileRegion referenceFileRegion) {
		try {
			OneByteWritableByteChannel channel = new OneByteWritableByteChannel();
			referenceFileRegion.transferTo(channel, 0L);
			return new String(channel.getResult(), Codec.defaultCharset);
		} catch (IOException e) {
			throw new IllegalStateException(String.format("[read]%s", referenceFileRegion), e);
		}
	}

	private static class OneByteWritableByteChannel implements WritableByteChannel {

		private final ByteArrayOutputStream output = new ByteArrayOutputStream();

		private boolean open = true;

		@Override
		public int write(ByteBuffer src) {
			if (!src.hasRemaining()) {
				return 0;
			}
			output.write(src.get());
			return 1;
		}

		@Override
		public boolean isOpen() {
			return open;
		}

		@Override
		public void close() {
			open = false;
		}

		byte[] getResult() {
			return output.toByteArray();
		}
	}

	@Test
	public void testIndex() throws Exception {
		Mockito.when(gtidCmdFilter.gtidSetContains(anyString(), anyLong())).thenReturn(false);
		initCommandStore(18067200);

		String filePath = "src/test/resources/GtidTest/appendonly.aof";
		int length = 1024 * 8;
		try (FileInputStream fis = new FileInputStream(filePath);
			 FileChannel fileChannel = fis.getChannel()) {
			fileChannel.position(0);
			while ((int)fileChannel.size() -(int)fileChannel.position() > 0) {
				length = Math.min(1024, (int)fileChannel.size() -(int)fileChannel.position());
				ByteBuffer buffer = ByteBuffer.allocate((int)length);
				int bytesRead = fileChannel.read(buffer);
				buffer.flip();
				if (bytesRead != -1) {
					ByteBuf byteBuf = Unpooled.wrappedBuffer(buffer.array());
					commandStore.appendCommands(byteBuf);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testIndexOffsetCorrectnessWithRotation() throws Exception {
		writeCmdWithRotation();
	}

	@Test
	public void testCoalescedNotifyWakesReaderByTimerOnMasterEventLoop() throws Exception {
		NioEventLoopGroup masterEventLoopGroup = new NioEventLoopGroup(1);
		try {
			TestKeeperConfig keeperConfig = new TestKeeperConfig()
					.setCommandOffsetNotifyBytesThreshold(1 << 20)
					.setCommandOffsetNotifyTimeMilliThreshold(300);
			openCommandStore(new DefaultCommandStore(ckStore, masterEventLoopGroup, keeperConfig,
					commandTemplate, maxFileSize, () -> false, () -> 3600, 0, () -> 20,
					DEFAULT_COMMAND_READER_FLYING_THRESHOLD, () -> true,
					commandReaderWriterFactory, createkeeperMonitor(), opParser, gtidCmdFilter, false,
					0L, asyncFileSystem(), () -> AsyncCommandStore.DEFAULT_ASYNC_WRITE_MAX_BYTES, getReplId()));

			long begin = System.nanoTime();
			Assert.assertEquals(10, commandStore.appendCommands(Unpooled.wrappedBuffer(new byte[10])));
			Assert.assertEquals(10, commandStore.totalLength());

			Assert.assertFalse("below bytes threshold must not notify at once", commandStore.awaitCommandsOffset(9, 50));
			Assert.assertTrue("timer on master event loop must notify", commandStore.awaitCommandsOffset(9, 5000));
			Assert.assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin) >= 250);
		} finally {
			closeCommandStore();
			masterEventLoopGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
		}
	}

	@Test
	public void testMultiChunkWriteNotifiesOnceAndFailedWriteDoesNot() throws Exception {
		int chunkBytes = 16;
		openCommandStore(createDefaultCommandStore(null, getKeeperConfig(), commandTemplate, maxFileSize, () -> false,
				() -> 3600, 0, () -> 20, DEFAULT_COMMAND_READER_FLYING_THRESHOLD, () -> true,
				commandReaderWriterFactory, createkeeperMonitor(), opParser, gtidCmdFilter, false, () -> chunkBytes));
		CoalescingOffsetNotifier coalescer = Mockito.mock(CoalescingOffsetNotifier.class);
		ReflectionTestUtils.setField(commandStore, "offsetNotifyCoalescer", coalescer);

		int length = chunkBytes * 6 + 5;
		Assert.assertEquals(length, commandStore.appendCommands(Unpooled.wrappedBuffer(new byte[length])));
		Assert.assertEquals(length, commandStore.totalLength());
		Mockito.verify(coalescer, Mockito.times(1)).onWritten(length, length - 1);
		Mockito.verifyNoMoreInteractions(coalescer);

		CommandWriter realWriter = commandStore.getCommandWriter();
		CommandWriter failingWriter = Mockito.mock(CommandWriter.class);
		Mockito.when(failingWriter.write(Mockito.any(ByteBuf.class))).thenThrow(new IOException("inject write fail"));
		ReflectionTestUtils.setField(commandStore, "cmdWriter", failingWriter);
		try {
			commandStore.appendCommands(Unpooled.wrappedBuffer(new byte[10]));
			Assert.fail("write failure must propagate");
		} catch (IOException expected) {
			Assert.assertEquals("inject write fail", expected.getMessage());
		} finally {
			ReflectionTestUtils.setField(commandStore, "cmdWriter", realWriter);
		}
		Mockito.verifyNoMoreInteractions(coalescer);
	}

	/** spec §13.21: writer 只负责写，绕过 onlyAppendCommand 直接写 writer 不会唤醒 reader。 */
	@Test
	public void testWriterDoesNotNotifyOffset() throws Exception {
		openCommandStore(createDefaultCommandStore(null, getKeeperConfig(), commandTemplate, maxFileSize, () -> false,
				() -> 3600, 0, () -> 20, DEFAULT_COMMAND_READER_FLYING_THRESHOLD, () -> false,
				commandReaderWriterFactory, createkeeperMonitor(), opParser, gtidCmdFilter, false,
				() -> AsyncCommandStore.DEFAULT_ASYNC_WRITE_MAX_BYTES));

		Assert.assertEquals(10, commandStore.getCommandWriter().write(Unpooled.wrappedBuffer(new byte[10])));
		Assert.assertEquals(10, commandStore.totalLength());
		Assert.assertFalse("writer must not notify", commandStore.awaitCommandsOffset(9, 50));

		Assert.assertEquals(1, commandStore.appendCommands(Unpooled.wrappedBuffer(new byte[1])));
		Assert.assertTrue("onlyAppendCommand notifies the end offset", commandStore.awaitCommandsOffset(10, 0));
	}

	/** spec §13.21: 有 master event loop 但开关关闭时，每次写入立即通知，不走定时。 */
	@Test
	public void testCoalescingDisabledNotifiesEachWriteWithMasterEventLoop() throws Exception {
		NioEventLoopGroup masterEventLoopGroup = new NioEventLoopGroup(1);
		try {
			TestKeeperConfig keeperConfig = new TestKeeperConfig()
					.setCommandOffsetNotifyBytesThreshold(1 << 20)
					.setCommandOffsetNotifyTimeMilliThreshold(60000);
			openCoalescingCommandStore(masterEventLoopGroup, keeperConfig, () -> false);

			Assert.assertEquals(10, commandStore.appendCommands(Unpooled.wrappedBuffer(new byte[10])));
			Assert.assertTrue(commandStore.awaitCommandsOffset(9, 0));
			Assert.assertEquals(5, commandStore.appendCommands(Unpooled.wrappedBuffer(new byte[5])));
			Assert.assertTrue(commandStore.awaitCommandsOffset(14, 0));
		} finally {
			closeCommandStore();
			masterEventLoopGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
		}
	}

	/** spec §13.21: 运行期修改开关与两个阈值，下一次写入即生效（阈值从 keeperConfig 每次读取）。 */
	@Test
	public void testSwitchAndThresholdsTakeEffectOnNextWrite() throws Exception {
		NioEventLoopGroup masterEventLoopGroup = new NioEventLoopGroup(1);
		try {
			TestKeeperConfig keeperConfig = new TestKeeperConfig()
					.setCommandOffsetNotifyBytesThreshold(1 << 20)
					.setCommandOffsetNotifyTimeMilliThreshold(60000);
			AtomicBoolean enabled = new AtomicBoolean(true);
			openCoalescingCommandStore(masterEventLoopGroup, keeperConfig, enabled::get);

			// 不足 bytes 阈值、time 阈值 60s：不通知
			commandStore.appendCommands(Unpooled.wrappedBuffer(new byte[10]));
			Assert.assertFalse(commandStore.awaitCommandsOffset(9, 100));

			// 调小 bytes 阈值：下一次写入累计 pending 达到阈值，立即通知，并取消 60s 的定时
			keeperConfig.setCommandOffsetNotifyBytesThreshold(8);
			commandStore.appendCommands(Unpooled.wrappedBuffer(new byte[1]));
			Assert.assertTrue(commandStore.awaitCommandsOffset(10, 0));

			// bytes 阈值调回、time 阈值调小：下一次写入按新 time 阈值排定时
			keeperConfig.setCommandOffsetNotifyBytesThreshold(1 << 20);
			keeperConfig.setCommandOffsetNotifyTimeMilliThreshold(50);
			commandStore.appendCommands(Unpooled.wrappedBuffer(new byte[1]));
			Assert.assertTrue("timer with new time threshold", commandStore.awaitCommandsOffset(11, 5000));

			// 关闭开关：下一次写入立即通知
			keeperConfig.setCommandOffsetNotifyTimeMilliThreshold(60000);
			enabled.set(false);
			commandStore.appendCommands(Unpooled.wrappedBuffer(new byte[1]));
			Assert.assertTrue(commandStore.awaitCommandsOffset(12, 0));
		} finally {
			closeCommandStore();
			masterEventLoopGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
		}
	}

	/** spec §13.22 / §15.2: close 先发出 pending 通知，再关闭 cmdWriter。 */
	@Test
	public void testCloseNotifiesPendingBeforeClosingWriter() throws Exception {
		NioEventLoopGroup masterEventLoopGroup = new NioEventLoopGroup(1);
		try {
			TestKeeperConfig keeperConfig = new TestKeeperConfig()
					.setCommandOffsetNotifyBytesThreshold(1 << 20)
					.setCommandOffsetNotifyTimeMilliThreshold(60000);
			openCoalescingCommandStore(masterEventLoopGroup, keeperConfig, () -> true);

			commandStore.appendCommands(Unpooled.wrappedBuffer(new byte[10]));
			Assert.assertFalse(commandStore.awaitCommandsOffset(9, 50));

			OffsetNotifier notifier = (OffsetNotifier) ReflectionTestUtils.getField(commandStore, "offsetNotifier");
			CommandWriter realWriter = commandStore.getCommandWriter();
			CommandWriter closeObserver = Mockito.mock(CommandWriter.class);
			AtomicBoolean notifiedBeforeWriterClose = new AtomicBoolean(false);
			Mockito.doAnswer(inv -> {
				notifiedBeforeWriterClose.set(notifier.await(9, 0));
				realWriter.close();
				return null;
			}).when(closeObserver).close();
			ReflectionTestUtils.setField(commandStore, "cmdWriter", closeObserver);

			closeCommandStore();

			Mockito.verify(closeObserver).close();
			Assert.assertTrue("pending offset must be notified before cmdWriter close", notifiedBeforeWriterClose.get());
		} finally {
			closeCommandStore();
			masterEventLoopGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
		}
	}

	/** spec §13.22: master event loop 已关闭、schedule 被拒时，写入照常成功并立即通知。 */
	@Test
	public void testScheduleRejectedStillWritesAndNotifies() throws Exception {
		NioEventLoopGroup masterEventLoopGroup = new NioEventLoopGroup(1);
		try {
			TestKeeperConfig keeperConfig = new TestKeeperConfig()
					.setCommandOffsetNotifyBytesThreshold(1 << 20)
					.setCommandOffsetNotifyTimeMilliThreshold(60000);
			openCoalescingCommandStore(masterEventLoopGroup, keeperConfig, () -> true);
			masterEventLoopGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();

			Assert.assertEquals(10, commandStore.appendCommands(Unpooled.wrappedBuffer(new byte[10])));
			Assert.assertEquals(10, commandStore.totalLength());
			Assert.assertTrue("rejected schedule falls back to immediate notify", commandStore.awaitCommandsOffset(9, 0));
		} finally {
			closeCommandStore();
			masterEventLoopGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
		}
	}

	private void openCoalescingCommandStore(NioEventLoopGroup masterEventLoopGroup, TestKeeperConfig keeperConfig,
											BooleanSupplier coalescingEnabled) throws IOException {
		openCommandStore(new DefaultCommandStore(ckStore, masterEventLoopGroup, keeperConfig,
				commandTemplate, maxFileSize, () -> false, () -> 3600, 0, () -> 20,
				DEFAULT_COMMAND_READER_FLYING_THRESHOLD, coalescingEnabled,
				commandReaderWriterFactory, createkeeperMonitor(), opParser, gtidCmdFilter, false,
				0L, asyncFileSystem(), () -> AsyncCommandStore.DEFAULT_ASYNC_WRITE_MAX_BYTES, getReplId()));
	}

	/**
	 * T-H3.CP6.3: cmdWriter / indexStore close failures must not skip
	 * {@code closeHandle(asyncSegmentFile)}.
	 */
	@Test
	public void closeStillClosesSegmentWhenPriorStepsFail() throws Exception {
		AsyncFileSystem spyFs = Mockito.spy(asyncFileSystem());
		openCommandStore(new DefaultCommandStore(null, getKeeperConfig(), commandTemplate, maxFileSize,
				() -> false, () -> 3600, 0, () -> 20, DEFAULT_COMMAND_READER_FLYING_THRESHOLD, () -> true,
				commandReaderWriterFactory, createkeeperMonitor(), opParser, gtidCmdFilter, true, 0L,
				spyFs, () -> AsyncCommandStore.DEFAULT_ASYNC_WRITE_MAX_BYTES, getReplId()));

		AsyncSegmentFile segment = commandStore.getAsyncSegmentFile();
		Assert.assertNotNull(segment);

		IndexStore realIndex = (IndexStore) ReflectionTestUtils.getField(commandStore, "indexStore");
		CommandWriter realWriter = commandStore.getCommandWriter();

		IndexStore failingIndex = Mockito.mock(IndexStore.class);
		CommandWriter failingWriter = Mockito.mock(CommandWriter.class);
		Mockito.doThrow(new IOException("index boom")).when(failingIndex).close();
		Mockito.doThrow(new IOException("writer boom")).when(failingWriter).close();

		ReflectionTestUtils.setField(commandStore, "cmdWriter", failingWriter);
		ReflectionTestUtils.setField(commandStore, "indexStore", failingIndex);

		try {
			commandStore.close();
			Mockito.verify(failingWriter).close();
			Mockito.verify(failingIndex).close();
			Mockito.verify(spyFs, Mockito.atLeastOnce()).close(segment);
		} finally {
			closeQuietly(realIndex);
			closeQuietly(realWriter);
			commandStore = null;
		}
	}

	private void closeQuietly(AutoCloseable closeable) {
		if (closeable == null) {
			return;
		}
		try {
			closeable.close();
		} catch (Exception e) {
			System.out.println("[closeQuietly] cleanup " + closeable + ": " + e);
		}
	}

	private void writeCmdWithRotation() throws Exception {
		// 1. index-enabled CommandStore with a small maxFileSize
		int smallMaxFileSize = 300;
		failed.set(0);
		Mockito.when(gtidCmdFilter.gtidSetContains(anyString(), anyLong())).thenReturn(false);
		TestKeeperConfig keeperConfig = new TestKeeperConfig();

		openCommandStore(new DefaultCommandStore(ckStore, keeperConfig,
				commandTemplate, smallMaxFileSize, () -> false, () -> 3600, 0, () -> 20,
				DEFAULT_COMMAND_READER_FLYING_THRESHOLD, () -> true,
				commandReaderWriterFactory, createkeeperMonitor(), opParser, gtidCmdFilter, true,
				0L, asyncFileSystem(), () -> AsyncCommandStore.DEFAULT_ASYNC_WRITE_MAX_BYTES, getReplId()));

		// 用于记录每条命令的 GTID、完整字节内容以及预期的全局起始偏移
		class CmdRecord {
			String gtid;
			byte[] rawBytes;
			long expectedStartOffset; // 全局偏移
		}
		List<CmdRecord> records = new ArrayList<>();
		AtomicLong globalOffset = new AtomicLong(0);

		String uuid = "a4f566ef50a85e1119f17f9b746728b48609a2ab";
		int gno = 1;

		// 生成 GTID 命令的辅助函数（与 DefaultIndexStoreTest 中相同）
		java.util.function.BiFunction<String, String[], ByteBuf> makeGtidCmd = (gtid, args) -> {
			ByteBuf buf = Unpooled.buffer();
			int totalArgs = 3 + args.length;
			buf.writeByte('*');
			buf.writeBytes(String.valueOf(totalArgs).getBytes());
			buf.writeBytes("\r\n".getBytes());
			writeBulkString(buf, "GTID");
			writeBulkString(buf, gtid);
			writeBulkString(buf, "0");
			for (String arg : args) {
				writeBulkString(buf, arg);
			}
			return buf;
		};

		// 2. 写入命令，直到至少发生一次文件滚动
		boolean rotated = false;

		while (!rotated) {
			String gtidStr = uuid + ":" + gno;
			ByteBuf cmdBuf = makeGtidCmd.apply(gtidStr, new String[]{"SET", "key" + gno, "val" + gno});
			byte[] cmdBytes = new byte[cmdBuf.readableBytes()];
			cmdBuf.getBytes(cmdBuf.readerIndex(), cmdBytes);
			cmdBuf.readerIndex(0); // reset for write

			// 记录预期偏移（写入前的全局偏移）
			long expectedStart = globalOffset.get();

			commandStore.appendCommands(cmdBuf);

			// 记录命令信息
			CmdRecord rec = new CmdRecord();
			rec.gtid = gtidStr;
			rec.rawBytes = cmdBytes;
			rec.expectedStartOffset = expectedStart;
			records.add(rec);

			globalOffset.addAndGet(cmdBytes.length);
			gno++;

			Assert.assertEquals(globalOffset.get(), commandStore.totalLength());
			if (globalOffset.get() >= smallMaxFileSize) {
				commandStore.rotateFileIfNecessary();
				rotated = true;
			}
		}

		// 3. 滚动后继续写入几条命令，验证新文件的偏移也正确
		int cmdsAfterRotation = 3;
		for (int i = 0; i < cmdsAfterRotation; i++) {
			String gtidStr = uuid + ":" + gno;
			ByteBuf cmdBuf = makeGtidCmd.apply(gtidStr, new String[]{"SET", "key" + gno, "val" + gno});
			byte[] cmdBytes = new byte[cmdBuf.readableBytes()];
			cmdBuf.getBytes(cmdBuf.readerIndex(), cmdBytes);
			cmdBuf.readerIndex(0);

			long expectedStart = globalOffset.get();
			commandStore.appendCommands(cmdBuf);

			CmdRecord rec = new CmdRecord();
			rec.gtid = gtidStr;
			rec.rawBytes = cmdBytes;
			rec.expectedStartOffset = expectedStart;
			records.add(rec);

			globalOffset.addAndGet(cmdBytes.length);
			gno++;
		}

		// 4. 验证每条命令的索引偏移与文件内容匹配
		for (int i = 0; i < records.size(); i++) {
			CmdRecord rec = records.get(i);
			// 提取 gno
			long gnoVal = Long.parseLong(rec.gtid.substring(uuid.length() + 1));
			List<BacklogOffsetReplicationProgress> segments = commandStore.locateCmdSegment(uuid, gnoVal, gnoVal);
			Assert.assertFalse("Should find a segment for " + rec.gtid, segments.isEmpty());

			// 只取第一个匹配段（通常只有一个）
			BacklogOffsetReplicationProgress seg = segments.get(0);
			long startBacklogOffset = seg.getProgress();
			long endBacklogOffset = seg.getEndProgressExcluded();

			// 验证起始偏移与预期一致
			Assert.assertEquals("Start offset mismatch for " + rec.gtid,
					rec.expectedStartOffset, startBacklogOffset);

			byte[] fileData = readFileRange(commandStore, startBacklogOffset, endBacklogOffset);
			if (!Arrays.equals(rec.rawBytes, fileData)) {
				failed.addAndGet(1);
			}
		}

		Assert.assertEquals("Failed to read expected bytes", 0, failed.get());

		failed.set(0);
	}

	// 辅助方法：写入 RESP Bulk String
	private void writeBulkString(ByteBuf buf, String str) {
		buf.writeByte('$');
		buf.writeBytes(String.valueOf(str.length()).getBytes());
		buf.writeBytes("\r\n".getBytes());
		buf.writeBytes(str.getBytes());
		buf.writeBytes("\r\n".getBytes());
	}

	// 从 CommandStore 中读取指定全局偏移范围的字节（read-mode AsyncSegmentFile + position）
	private byte[] readFileRange(CommandStore store, long startOffset, long endOffset) throws IOException {
		AsyncCommandStore asyncStore = (AsyncCommandStore) store;
		AsyncFileSystem fs = asyncStore.getAsyncFileSystem();
		int length = (int) (endOffset - startOffset);
		byte[] data = new byte[0];
		AsyncSegmentFile readSeg = null;
		try {
			readSeg = AsyncFileSystemHelper.awaitOpen(fs,
					fs.open(asyncStore.getCommandBaseDir().getAbsolutePath(),
							asyncStore.getCommandFileNamePrefix(),
							asyncStore.getCommandIndexPrefixes(),
							false,
							asyncStore.getFileSystemReplId().toString()),
					"open read segment for test");
			AsyncFileSystemHelper.await(fs.position(readSeg, startOffset),
					"position read segment to " + startOffset);
			ByteBuf buf = AsyncFileSystemHelper.await(fs.read(readSeg, length),
					"read [" + startOffset + "," + endOffset + ")");
			try {
				data = new byte[buf.readableBytes()];
				buf.readBytes(data);
				if (data.length != length) {
					failed.addAndGet(1);
				}
			} finally {
				buf.release();
			}
		} catch (Exception e) {
			failed.addAndGet(1);
		} finally {
			if (readSeg != null) {
				AsyncFileSystemHelper.await(fs.close(readSeg), "close read segment for test");
			}
		}
		return data;
	}

}
