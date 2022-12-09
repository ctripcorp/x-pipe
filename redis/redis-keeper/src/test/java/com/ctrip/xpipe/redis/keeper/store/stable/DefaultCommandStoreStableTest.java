package com.ctrip.xpipe.redis.keeper.store.stable;

import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.store.CommandFile;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.CommandsListener;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.store.DefaultCommandStore;
import com.ctrip.xpipe.redis.keeper.store.cmd.OffsetCommandReaderWriterFactory;
import com.ctrip.xpipe.redis.core.store.OffsetReplicationProgress;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * @author wenchao.meng
 *
 *         Sep 12, 2016
 */
public class DefaultCommandStoreStableTest extends AbstractRedisKeeperTest {

	private DefaultCommandStore commandStore;

	private int maxFileSize = 1 << 10;

	private final int readerCount = 3;

	private StringComparator comparator = new StringComparator(readerCount);

	private DefaultCommandFuture<?> future = new DefaultCommandFuture<>();

	private OffsetCommandReaderWriterFactory commandReaderWriterFactory = new OffsetCommandReaderWriterFactory();

	@Before
	public void beforeDefaultCommandStoreTest() throws IOException, Exception {

		String testDir = getTestFileDir();
		File commandTemplate = new File(testDir, getTestName());
		commandStore = new DefaultCommandStore(commandTemplate, maxFileSize, commandReaderWriterFactory, createkeeperMonitor());
		commandStore.initialize();
	}

	@Test
	public void testReadWrite() throws IOException, InterruptedException, ExecutionException {

		executors.execute(new Writer(commandStore));
		for (int i = 0; i < readerCount; i++) {
			executors.execute(new Reader(i, commandStore));
		}
		future.get();
	}

	public class Reader extends AbstractExceptionLogTask {

		private CommandStore commandStore;
		private int readIndex;

		public Reader(int readIndex, CommandStore commandStore) {
			this.commandStore = commandStore;
			this.readIndex = readIndex;
		}

		@Override
		protected void doRun() {

			try {
				commandStore.addCommandsListener(new OffsetReplicationProgress(0), new CommandsListener() {

					@Override
					public ChannelFuture onCommand(CommandFile currentFile, long filePosition, Object cmd) {

						String result = readFileChannelInfoMessageAsString((ReferenceFileRegion) cmd);
						if (!comparator.compare(readIndex, result)) {
							future.setFailure(new Exception("not equals:" + result));
						}
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
				future.setFailure(e);
			}
		}
	}

	public class Writer implements Runnable {

		private CommandStore commandStore;

		public Writer(CommandStore commandStore) {
			this.commandStore = commandStore;
		}

		@Override
		public void run() {

			while (!Thread.interrupted()) {
				try {
					int length = randomInt(100, 500);
					String random = randomString(length);
					logger.debug("[run]{}", random.length());
					comparator.add(random);
					commandStore.appendCommands(Unpooled.wrappedBuffer(random.getBytes()));
				} catch (IOException e) {
					logger.error("[run]", e);
				}
			}
		}
	}
}
