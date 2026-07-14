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
import com.ctrip.xpipe.redis.keeper.store.ck.CKStore;
import com.ctrip.xpipe.redis.keeper.store.cmd.OffsetCommandReaderWriterFactory;
import com.ctrip.xpipe.redis.keeper.store.gtid.index.AbstractIndex;
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

	@Mock
	private NioEventLoopGroup nioEventLoopGroup;

	private AtomicInteger failed = new AtomicInteger(0);


	@Before
	public void beforeDefaultCommandStoreTest() throws Exception	 {

		String testDir = getTestFileDir();
		commandTemplate = new File(testDir, getTestName()+"_");
		RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
		RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
		opParser = new GeneralRedisOpParser(redisOpParserManager);
		commandStore = createDefaultCommandStore(commandTemplate, maxFileSize, commandReaderWriterFactory,
				createkeeperMonitor(), opParser, gtidCmdFilter);
		commandStore.initialize();
	}


	@Test
	public void testLoadIdxFromFile() throws Exception {
		File baseDir = new File("./src/test/resources/DefaultCommandStoreTest");
		String prefix = "abcdefg_";
		commandStore = createDefaultCommandStore(new File(baseDir, prefix), maxFileSize, commandReaderWriterFactory,
				createkeeperMonitor(), opParser, gtidCmdFilter);
		commandStore.initialize();
		List<CommandFileOffsetGtidIndex> idxList = commandStore.getIndexList();
		logger.info("[testLoadIdxFromFile] idxList {}", idxList);

		Assert.assertEquals(6, idxList.size());
	}

	@Test
	public void testDynamicConfig() throws Exception {

		final int initDataKeep = 20;
		final AtomicInteger dataKeep = new AtomicInteger(initDataKeep);
		int gcAfterCreateMilli = 60000;
		File commandTemplate = new File(getTestFileDir(), getTestName()+"_");

		commandStore = new DefaultCommandStore(null, getKeeperConfig(), commandTemplate, maxFileSize, () -> false, () -> 3600, gcAfterCreateMilli, () -> dataKeep.get(), DEFAULT_COMMAND_READER_FLYING_THRESHOLD, () -> true,
				commandReaderWriterFactory, createkeeperMonitor(), opParser, gtidCmdFilter, true, 0L, asyncFileSystem(), () -> AsyncCommandStore.DEFAULT_ASYNC_WRITE_MAX_BYTES, getReplId()){
			@Override
			public long totalLength() {
				return initDataKeep * maxFileSize;
			}
		};
		commandStore.initialize();

		Assert.assertFalse(commandStore.canDeleteCmdFile(maxFileSize * 10, 0, maxFileSize, new Date().getTime() - 600000));

		dataKeep.set(19);
		Assert.assertFalse(commandStore.canDeleteCmdFile(maxFileSize * 10, 0, maxFileSize, new Date().getTime() - 600000));

		dataKeep.set(18);
		Assert.assertTrue(commandStore.canDeleteCmdFile(maxFileSize * 10, 0, maxFileSize, new Date().getTime() - 600000));

	}

	@Test
	public void testNotifyImmediatelyWhenCoalescingDisabled() throws Exception {
		commandStore.close();
		commandStore = createDefaultCommandStore(null, getKeeperConfig(), commandTemplate, maxFileSize, () -> false, () -> 3600, 0, () -> 20,
				DEFAULT_COMMAND_READER_FLYING_THRESHOLD, () -> false, commandReaderWriterFactory, createkeeperMonitor(), opParser, gtidCmdFilter, true);
		commandStore.initialize();
		ReflectionTestUtils.setField(commandStore, "buildIndex", false);

		commandStore.appendCommands(Unpooled.wrappedBuffer(new byte[] { 'a' }));
		Assert.assertTrue(commandStore.awaitCommandsOffset(0, 10));
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
	public void testGetAsSoonAsMessageWritten() throws IOException, InterruptedException {

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

	@Test
	public void testConcurrentRotateGetFileLength() throws IOException, InterruptedException, ExecutionException, Exception {

		final AtomicReference<DefaultCommandStore> commandStore = new AtomicReference<>();
		final int appendCount = 10;

		try {
			String testDir = getTestFileDir();
			File commandTemplate = new File(testDir, getTestName()+"_");
			commandStore.set(createDefaultCommandStore(commandTemplate, 1, commandReaderWriterFactory, createkeeperMonitor(),
					opParser, gtidCmdFilter));
			commandStore.get().initialize();
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
	public void testReadWrite() throws IOException, InterruptedException {

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
		commandStore.close();
		commandStore = createDefaultCommandStore(null, getKeeperConfig(), commandTemplate, 5, () -> false, () -> 3600, 0,
				() -> 20, DEFAULT_COMMAND_READER_FLYING_THRESHOLD, () -> false, commandReaderWriterFactory,
				createkeeperMonitor(), opParser, gtidCmdFilter, false, () -> 2);
		commandStore.initialize();

		String expected = "abcdefg";
		commandStore.appendCommands(Unpooled.wrappedBuffer(expected.getBytes(Codec.defaultCharset)));

		Assert.assertEquals(expected.length(), commandStore.totalLength());
		Assert.assertTrue(new File(commandTemplate.getParentFile(), commandTemplate.getName() + "0").isFile());
		Assert.assertTrue(new File(commandTemplate.getParentFile(), commandTemplate.getName() + "5").isFile());
		Assert.assertTrue(new File(commandTemplate.getParentFile(), "idx_" + commandTemplate.getName() + "0").isFile());
		Assert.assertTrue(new File(commandTemplate.getParentFile(), "idx_" + commandTemplate.getName() + "5").isFile());
		Assert.assertEquals(expected, readCommandStoreTilNoMessage(commandStore, expected.length()));
	}


	@Test
	public void testGcOldCmdFile() throws Exception {
		AtomicInteger maxSecondsKeepCmdFile = new AtomicInteger(60);

		commandStore = createDefaultCommandStore(null, getKeeperConfig(), commandTemplate, 100, () -> false, maxSecondsKeepCmdFile::get, 0,
				() -> 20, DEFAULT_COMMAND_READER_FLYING_THRESHOLD, () -> true, commandReaderWriterFactory, createkeeperMonitor(),
				opParser, gtidCmdFilter, true);
		commandStore.initialize();
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
	public void testDelCmdFileDeletesIndexV2CompanionFiles() throws Exception {
		appendCommandsToStore(1, 50);

		File baseDir = commandTemplate.getParentFile();
		File[] cmdFiles = baseDir.listFiles((dir, name) -> name.startsWith(commandTemplate.getName()));
		Assert.assertNotNull(cmdFiles);
		Assert.assertEquals(1, cmdFiles.length);
		File cmdFile = cmdFiles[0];

		new File(baseDir, AbstractIndex.INDEX + cmdFile.getName()).createNewFile();
		new File(baseDir, AbstractIndex.BLOCK + cmdFile.getName()).createNewFile();
		new File(baseDir, AbstractIndex.INDEX_V2 + cmdFile.getName()).createNewFile();
		new File(baseDir, AbstractIndex.BLOCK_V2 + cmdFile.getName()).createNewFile();

		ReflectionTestUtils.invokeMethod(commandStore, "delCmdFile", cmdFile);

		Assert.assertFalse(cmdFile.exists());
		Assert.assertFalse(new File(baseDir, AbstractIndex.INDEX + cmdFile.getName()).exists());
		Assert.assertFalse(new File(baseDir, AbstractIndex.BLOCK + cmdFile.getName()).exists());
		Assert.assertFalse(new File(baseDir, AbstractIndex.INDEX_V2 + cmdFile.getName()).exists());
		Assert.assertFalse(new File(baseDir, AbstractIndex.BLOCK_V2 + cmdFile.getName()).exists());
	}

	@Test
	public void testGc() throws Exception {
		int fileNumToKeep = 2;

		commandStore = createDefaultCommandStore(null, getKeeperConfig(), commandTemplate, 100, () -> false, () -> 3600, 0,
				() -> fileNumToKeep, DEFAULT_COMMAND_READER_FLYING_THRESHOLD, () -> true, commandReaderWriterFactory,
				createkeeperMonitor(), opParser, gtidCmdFilter, true);
		commandStore.initialize();
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

		commandStore = createDefaultCommandStore(null, getKeeperConfig(), commandTemplate, 100, () -> false, () -> 3600, 0,
				() -> fileNumToKeep, DEFAULT_COMMAND_READER_FLYING_THRESHOLD, () -> true, commandReaderWriterFactory,
				createkeeperMonitor(), opParser, gtidCmdFilter, true);
		commandStore.initialize();
		appendCommandsToStore(3, 100);

		commandStore.retainCommands(buildCommandGuarantee(0, 100000, () -> true, () -> 0));
		appendCommandsToStore(1, 100);
		commandStore.gc();
		Assert.assertEquals(0, commandStore.lowestAvailableOffset());
	}

	@Test
	public void testRetainCommandsButTimeout() throws Exception {
		int fileNumToKeep = 2;

		commandStore = createDefaultCommandStore(null, getKeeperConfig(), commandTemplate, 100, () -> false, () -> 3600, 0,
				() -> fileNumToKeep, DEFAULT_COMMAND_READER_FLYING_THRESHOLD, () -> true,
				commandReaderWriterFactory, createkeeperMonitor(), opParser, gtidCmdFilter, true);
		commandStore.initialize();
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

		commandStore = createDefaultCommandStore(null, getKeeperConfig(), commandTemplate, 100, () -> false, () -> 3600, 0,
				() -> fileNumToKeep, DEFAULT_COMMAND_READER_FLYING_THRESHOLD, () -> true, commandReaderWriterFactory,
				createkeeperMonitor(), opParser, gtidCmdFilter, true);
		commandStore.initialize();
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

		commandStore = createDefaultCommandStore(null, getKeeperConfig(), commandTemplate, 100, () -> false, () -> 3600, 0,
				() -> fileNumToKeep, DEFAULT_COMMAND_READER_FLYING_THRESHOLD, () -> true,
				commandReaderWriterFactory, createkeeperMonitor(), opParser, gtidCmdFilter, true);
		commandStore.initialize();
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
		}, backlogOffset, timeoutMilli);
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

	@After
	public void afterDefaultCommandStoreTest() throws IOException {
		commandStore.close();
	}

	@Test
	public void testIndex() throws Exception {
		String testDir = getTestFileDir();
		commandTemplate = new File(testDir, getTestName()+"_");
		RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
		RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
		opParser = new GeneralRedisOpParser(redisOpParserManager);
		Mockito.when(gtidCmdFilter.gtidSetContains(anyString(), anyLong())).thenReturn(false);
		commandStore = createDefaultCommandStore(commandTemplate, 18067200, commandReaderWriterFactory, createkeeperMonitor(), opParser, gtidCmdFilter);
		commandStore.initialize();

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
	public void testIndexOffsetCorrectnessWithRotationAndFlush() throws Exception {
		writeCmdWithRotation(true);
	}

	@Test
	public void testIndexOffsetCorrectnessWithRotationAndNoFlush() throws Exception {
		writeCmdWithRotation(false);
	}

	private void writeCmdWithRotation(boolean flush) throws Exception {
		// 1. 构造一个启用索引、滑动窗口、且 maxFileSize 很小的 CommandStore
		int smallMaxFileSize = 300;
		failed.set(0);
		Mockito.when(gtidCmdFilter.gtidSetContains(anyString(), anyLong())).thenReturn(false);
		TestKeeperConfig keeperConfig = createRotationTestKeeperConfig(flush);
		Mockito.when(ckStore.getKeeperConfig()).thenReturn(keeperConfig);
		Mockito.when(ckStore.getMasterEventLoop()).thenReturn(nioEventLoopGroup);

		commandTemplate = new File(getTestFileDir(), getTestName()+"_");
		commandStore = createDefaultCommandStore(ckStore, keeperConfig,
				commandTemplate, smallMaxFileSize, () -> false, () -> 3600, 0, () -> 20,
				DEFAULT_COMMAND_READER_FLYING_THRESHOLD, () -> true,
				commandReaderWriterFactory, createkeeperMonitor(), opParser, gtidCmdFilter, true);
		commandStore.initialize();

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

			// 写入命令（内部会走滑动窗口，可能不立即落盘）
			commandStore.appendCommands(cmdBuf);

			// 记录命令信息
			CmdRecord rec = new CmdRecord();
			rec.gtid = gtidStr;
			rec.rawBytes = cmdBytes;
			rec.expectedStartOffset = expectedStart;
			records.add(rec);

			globalOffset.addAndGet(cmdBytes.length);
			gno++;

			// 与索引侧 StreamCommandReader.currentOffset 一致；totalLength() 不含滑动窗口 pending
			if (globalOffset.get() >= smallMaxFileSize) {
				commandStore.flushSlidingWindow();
				commandStore.rotateFileIfNecessary();
				rotated = true;
			}
		}
		int recordsBeforeRotation = records.size();

		// no-flush 场景：滚动完成后再 mock，仅拦截滚动后的 flush
		if (!flush) {
			DefaultCommandStore store = this.commandStore;
			CommandStore spy = Mockito.spy(store);
			Mockito.doNothing().when(spy).flushSlidingWindow();
			this.commandStore = (DefaultCommandStore) spy;
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

		// 确保最终所有数据落盘（close 前会 flush）
		commandStore.flushSlidingWindow();

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

			// flush 场景校验全部；no-flush 仅校验滚动后仍在窗口内的命令（滚动前已落盘）
			if (flush || i >= recordsBeforeRotation) {
				byte[] fileData = readFileRange(commandStore, startBacklogOffset, endBacklogOffset);
				if (!Arrays.equals(rec.rawBytes, fileData)) {
					failed.addAndGet(1);
				}
			}
		}

		if(flush){
			Assert.assertTrue("flush Failed to read expected bytes",failed.get() == 0);
		}else {
			Assert.assertTrue("no flush Failed to read expected bytes", failed.get() > 0);
		}

		// 清理
		failed.set(0);
		commandStore.close();
	}

	/**
	 * no-flush 场景需禁用 TimerSlidingWindow 按时间的自动刷盘（默认 4ms），
	 * 否则整包跑时写入间隔变长会在 mock flushSlidingWindow 之前落盘，导致 failed==0 误判。
	 * batch 阈值保持默认即可，旋转条件用 globalOffset 而非 totalLength()。
	 */
	private TestKeeperConfig createRotationTestKeeperConfig(boolean flush) {
		if (flush) {
			return new TestKeeperConfig();
		}
		return new TestKeeperConfig() {
			@Override
			public long getCmdBatchFlushIntervalMillis() {
				return 3_600_000L;
			}
		};
	}

	// 辅助方法：写入 RESP Bulk String
	private void writeBulkString(ByteBuf buf, String str) {
		buf.writeByte('$');
		buf.writeBytes(String.valueOf(str.length()).getBytes());
		buf.writeBytes("\r\n".getBytes());
		buf.writeBytes(str.getBytes());
		buf.writeBytes("\r\n".getBytes());
	}

	// 从 CommandStore 中读取指定全局偏移范围的字节
	private byte[] readFileRange(CommandStore store, long startOffset, long endOffset) throws IOException {
		// 先找到包含 startOffset 的文件
		CommandFile cf = store.findFileForOffset(startOffset);
		Assert.assertNotNull("No file for offset " + startOffset, cf);
		byte[] data = new byte[0];
		try {
			File file = cf.getFile();
			long fileStartOffset = cf.getStartOffset();
			long localStart = startOffset - fileStartOffset;
			int length = (int) (endOffset - startOffset);

			data = new byte[length];
			try (FileInputStream fis = new FileInputStream(file);
				 FileChannel channel = fis.getChannel()) {
				channel.position(localStart);
				ByteBuffer buf = ByteBuffer.allocate(length);
				int read = channel.read(buf);
				if (length != read) {
					failed.addAndGet(1);
				}
				buf.flip();
				buf.get(data);
			}
		}catch (Exception e) {
			failed.addAndGet(1);
		}
		return data;
	}

}
