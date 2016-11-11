package com.ctrip.xpipe.redis.keeper.store;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.store.CommandsListener;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.google.common.util.concurrent.SettableFuture;

import io.netty.buffer.Unpooled;

/**
 * @author wenchao.meng
 *
 *         Sep 12, 2016
 */
public class DefaultCommandStoreTest extends AbstractRedisKeeperTest {

	private DefaultCommandStore commandStore;

	private int maxFileSize = 1 << 10;

	private int minWritten = (1 << 20);

	@Before
	public void beforeDefaultCommandStoreTest() throws IOException {

		String testDir = getTestFileDir();
		File commandTemplate = new File(testDir, getTestName());
		commandStore = new DefaultCommandStore(commandTemplate, maxFileSize);
	}

	@Test
	public void testGetAsSoonAsMessageWritten() throws IOException, InterruptedException {

		final StringBuilder sb = new StringBuilder();
		final Semaphore semaphore = new Semaphore(0);

		executors.execute(new AbstractExceptionLogTask() {

			@Override
			protected void doRun() throws Exception {
				commandStore.addCommandsListener(0, new CommandsListener() {

					@Override
					public void onCommand(ReferenceFileRegion referenceFileRegion) {

						sb.append(readFileChannelInfoMessageAsString(referenceFileRegion));
						semaphore.release();
					}

					@Override
					public boolean isOpen() {
						return true;
					}

					@Override
					public void beforeCommand() {

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
			commandStore.set(new DefaultCommandStore(commandTemplate, 1));
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
					commandStore.addCommandsListener(offset, new CommandsListener() {

						@Override
						public void onCommand(ReferenceFileRegion referenceFileRegion) {

							logger.debug("[onCommand]{}", referenceFileRegion);
							result.append(readFileChannelInfoMessageAsString(referenceFileRegion));
							semaphore.release((int) referenceFileRegion.count());
						}

						@Override
						public boolean isOpen() {
							return true;
						}

						@Override
						public void beforeCommand() {

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

	@After
	public void afterDefaultCommandStoreTest() throws IOException {
		commandStore.close();
	}

}
