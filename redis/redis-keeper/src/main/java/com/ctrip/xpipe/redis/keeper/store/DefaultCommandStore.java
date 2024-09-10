package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.utils.CloseState;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.function.IntSupplier;

/**
 * @author qing.gu
 *
 *         Aug 9, 2016
 */
public class DefaultCommandStore extends AbstractCommandStore implements CommandStore {

	private static final Logger logger = LoggerFactory.getLogger(DefaultCommandStore.class);

	public DefaultCommandStore(File file, int maxFileSize, CommandReaderWriterFactory cmdReaderWriterFactory, KeeperMonitor keeperMonitor) throws IOException {
		this(file, maxFileSize, () -> 12 * 3600, 3600*1000, () -> 20, DEFAULT_COMMAND_READER_FLYING_THRESHOLD, cmdReaderWriterFactory, keeperMonitor);
	}

	public DefaultCommandStore(File file, int maxFileSize, IntSupplier maxTimeSecondKeeperCmdFileAfterModified,
										   int minTimeMilliToGcAfterModified, IntSupplier fileNumToKeep,
										   long commandReaderFlyingThreshold,
										   CommandReaderWriterFactory cmdReaderWriterFactory,
										   KeeperMonitor keeperMonitor) throws IOException {
		super(file, maxFileSize, maxTimeSecondKeeperCmdFileAfterModified, minTimeMilliToGcAfterModified, fileNumToKeep,
				commandReaderFlyingThreshold, cmdReaderWriterFactory, keeperMonitor);
	}

	private CommandReader<ReferenceFileRegion> beginRead(OffsetReplicationProgress replicationProgress) throws IOException {

		makeSureOpen();

		CommandReader<ReferenceFileRegion> reader = cmdReaderWriterFactory.createCmdReader(replicationProgress, this,
				offsetNotifier, commandReaderFlyingThreshold);
		addReader(reader);
		return reader;
	}

	@Override
	public void addCommandsListener(ReplicationProgress<?> progress, final CommandsListener listener) throws IOException {

		if (!(progress instanceof OffsetReplicationProgress)) {
			throw new UnsupportedOperationException("unsupported progress " + progress);
		}

		makeSureOpen();
		logger.info("[addCommandsListener][begin] from offset {}, {}", progress, listener);

		CommandReader<ReferenceFileRegion> cmdReader = null;

		try {
			cmdReader = beginRead((OffsetReplicationProgress) progress);
		} finally {
			// ensure beforeCommand() is always called
			listener.beforeCommand();
		}

		logger.info("[addCommandsListener] from {}, {}", progress, cmdReader);

		try {
			while (listener.isOpen() && !Thread.currentThread().isInterrupted()) {

				final ReferenceFileRegion referenceFileRegion = cmdReader.read(1000);
				if (null == referenceFileRegion) continue;

				logger.debug("[addCommandsListener] {}", referenceFileRegion);

				if(getDelayTraceLogger().isDebugEnabled()){
					getDelayTraceLogger().debug("[write][begin]{}, {}", listener, referenceFileRegion.getTotalPos());
				}
				getCommandStoreDelay().beginSend(listener, referenceFileRegion.getTotalPos());

				ChannelFuture future = null;
				try {
					future = listener.onCommand(cmdReader.getCurCmdFile(), cmdReader.position(), referenceFileRegion);
				} catch (CloseState.CloseStateException e) {
					logger.info("[addCommandsListener][listener closed] deallocate fileRegion");
					referenceFileRegion.deallocate();
					throw e;
				}

				if(future != null){
					CommandReader<ReferenceFileRegion> finalCmdReader = cmdReader;
					future.addListener(new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) throws Exception {

							finalCmdReader.flushed(referenceFileRegion);
							getCommandStoreDelay().flushSucceed(listener, referenceFileRegion.getTotalPos());
							if(logger.isDebugEnabled()){
								getDelayTraceLogger().debug("[write][ end ]{}, {}", listener, referenceFileRegion.getTotalPos());
							}
						}
					});
				}

				if (referenceFileRegion.count() <= 0) {
					try {
						Thread.sleep(1);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
			}
		} catch (Throwable th) {
			logger.error("[readCommands][exit]" + listener, th);
		} finally {
			cmdReader.close();
		}
		logger.info("[addCommandsListener][end] from {}, {}", progress, listener);
	}

	@Override
	public Logger getLogger() {
		return logger;
	}

}
