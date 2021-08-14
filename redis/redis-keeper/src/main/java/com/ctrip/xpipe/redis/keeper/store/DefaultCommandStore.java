package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileChannel;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.store.CommandsGuarantee;
import com.ctrip.xpipe.redis.core.store.CommandReader;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.CommandsListener;
import com.ctrip.xpipe.redis.keeper.monitor.CommandStoreDelay;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.util.KeeperLogger;
import com.ctrip.xpipe.utils.DefaultControllableFile;
import com.ctrip.xpipe.utils.Gate;
import com.ctrip.xpipe.utils.OffsetNotifier;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

/**
 * @author qing.gu
 *
 *         Aug 9, 2016
 */
public class DefaultCommandStore extends AbstractStore implements CommandStore {

	private final static Logger logger = LoggerFactory.getLogger(DefaultCommandStore.class);
	
	private final static Logger delayTraceLogger = KeeperLogger.getDelayTraceLog();

	public static final long DEFAULT_COMMAND_READER_FLYING_THRESHOLD = 1 << 15;

	private final File baseDir;

	private final String fileNamePrefix;

	private final int maxFileSize;
	
	private final IntSupplier fileNumToKeep;
	private final int minTimeMilliToGcAfterModified;

	private final IntSupplier maxTimeSecondKeeperCmdFileAfterModified;

	private final FilenameFilter fileFilter;

	private final ConcurrentMap<DefaultCommandReader, Boolean> readers = new ConcurrentHashMap<>();

	private final OffsetNotifier offsetNotifier;

	private final long commandReaderFlyingThreshold;

	private AtomicReference<CommandFileContext> cmdFileCtxRef = new AtomicReference<>();
	private Object cmdFileCtxRefLock = new Object();
	
	private CommandStoreDelay commandStoreDelay;

	private List<CommandsGuarantee> commandsGuarantees = new CopyOnWriteArrayList<>();

	private ReentrantLock gcLock = new ReentrantLock();

	public DefaultCommandStore(File file, int maxFileSize, KeeperMonitor keeperMonitor) throws IOException {
		this(file, maxFileSize, () -> 12 * 3600, 3600*1000, () -> 20, DEFAULT_COMMAND_READER_FLYING_THRESHOLD, keeperMonitor);
	}

	public DefaultCommandStore(File file, int maxFileSize, IntSupplier maxTimeSecondKeeperCmdFileAfterModified, int minTimeMilliToGcAfterModified, IntSupplier fileNumToKeep, long commandReaderFlyingThreshold, KeeperMonitor keeperMonitor) throws IOException {
		
		this.baseDir = file.getParentFile();
		this.fileNamePrefix = file.getName();
		this.maxFileSize = maxFileSize;
		this.maxTimeSecondKeeperCmdFileAfterModified = maxTimeSecondKeeperCmdFileAfterModified;
		this.fileNumToKeep = fileNumToKeep;
		this.commandReaderFlyingThreshold = commandReaderFlyingThreshold;
		this.minTimeMilliToGcAfterModified = minTimeMilliToGcAfterModified;
		this.commandStoreDelay = keeperMonitor.createCommandStoreDelay(this);
		
		fileFilter = new PrefixFileFilter(fileNamePrefix);

		long currentStartOffset = findMaxStartOffset();
		File currentFile = fileForStartOffset(currentStartOffset);
		logger.info("Write to {}", currentFile.getName());
		CommandFileContext cmdFileCtx = new CommandFileContext(currentStartOffset, currentFile);
		cmdFileCtxRef.set(cmdFileCtx);
		offsetNotifier = new OffsetNotifier(cmdFileCtx.totalLength() - 1);
	}

	private File fileForStartOffset(long startOffset) {
		return new File(baseDir, fileNamePrefix + startOffset);
	}

	private long findMaxStartOffset() {
		
		long maxStartOffset = 0;
		File[] files = allFiles();
		if (files != null) {
			for (File file : files) {
				long startOffset = extractStartOffset(file);
				if (startOffset > maxStartOffset) {
					maxStartOffset = startOffset;
				}
			}
		}
		return maxStartOffset;
	}

	private File[] allFiles() {
		File []files = baseDir.listFiles((FilenameFilter) fileFilter);
		if(files == null){
			files = new File[0];
		}
		return files;
	}

	private long extractStartOffset(File file) {
		return Long.parseLong(file.getName().substring(fileNamePrefix.length()));
	}

	@Override
	public int appendCommands(ByteBuf byteBuf) throws IOException {
		
		makeSureOpen();

		rotateFileIfNenessary();

		CommandFileContext cmdFileCtx = cmdFileCtxRef.get();

		//delay monitor
		if(delayTraceLogger.isDebugEnabled()) {
			delayTraceLogger.debug("[appendCommands][begin]{}");
		}

		commandStoreDelay.beginWrite();
		
		int wrote = ByteBufUtils.writeByteBufToFileChannel(byteBuf, cmdFileCtx.getChannel(), delayTraceLogger);

		if(delayTraceLogger.isDebugEnabled()){
			logger.debug("[appendCommands]{}, {}, {}", cmdFileCtx, byteBuf.readableBytes(), cmdFileCtx.fileLength());
		}

		long offset = cmdFileCtx.totalLength() - 1;
		
		//delay monitor
		if(delayTraceLogger.isDebugEnabled()){
			delayTraceLogger.debug("[appendCommands][ end ]{}", offset + 1);
		}
		commandStoreDelay.endWrite(offset + 1);

		offsetNotifier.offsetIncreased(offset);
		
		return wrote;
	}

	@Override
	public long totalLength() {
		synchronized (cmdFileCtxRefLock) {
			return cmdFileCtxRef.get().totalLength();
		}
	}
	
	private void rotateFileIfNenessary() throws IOException {

		CommandFileContext curCmdFileCtx = cmdFileCtxRef.get();
		if (curCmdFileCtx.fileLength() >= maxFileSize) {
			long newStartOffset = curCmdFileCtx.totalLength();
			File newFile = new File(baseDir, fileNamePrefix + newStartOffset);
			logger.info("Rotate to {}", newFile.getName());
			synchronized (cmdFileCtxRefLock) {
				
				CommandFileContext newCmdFileCtx = new CommandFileContext(newStartOffset, newFile);
				newCmdFileCtx.createIfNotExist();
				cmdFileCtxRef.set(newCmdFileCtx);
				
				curCmdFileCtx.close();
			}
		}
	}

	@Override
	public CommandReader beginRead(long startOffset) throws IOException {

		makeSureOpen();

		File targetFile = findFileForOffset(startOffset);
		if (targetFile == null) {
			throw new IOException("File for offset " + startOffset + " in dir " + baseDir + " does not exist");
		}
		long fileStartOffset = extractStartOffset(targetFile);
		long channelPosition = startOffset - fileStartOffset;
		DefaultCommandReader reader = new DefaultCommandReader(targetFile, channelPosition, commandReaderFlyingThreshold);
		readers.put(reader, Boolean.TRUE);
		return reader;
	}

	private File findFileForOffset(long targetStartOffset) throws IOException {

		rotateFileIfNenessary();

		File[] files = baseDir.listFiles((FilenameFilter) fileFilter);
		if (files != null) {
			for (File file : files) {
				long startOffset = extractStartOffset(file);
				if (targetStartOffset >= startOffset && (targetStartOffset < startOffset + file.length()
						|| targetStartOffset < startOffset + maxFileSize)) {
					return file;
				}
			}
		}

		if (files != null) {
			for (File file : files) {
				logger.info("[findFileForOffset]{}, {}", file.getName(), file.length());
			}
		}
		return null;
	}

	private File findNextFile(File curFile) {
		long startOffset = extractStartOffset(curFile);
		File nextFile = fileForStartOffset(startOffset + curFile.length());
		if (nextFile.isFile()) {
			return nextFile;
		} else {
			return null;
		}
	}

	private class DefaultCommandReader implements CommandReader {

		private final Logger logger = LoggerFactory.getLogger(DefaultCommandReader.class);

		private File curFile;

		private long curPosition;

		private ReferenceFileChannel referenceFileChannel;

		private AtomicLong flying = new AtomicLong(0);

		private long flyingThreshhold = 1 << 15;

		private Gate gate;

		public DefaultCommandReader(File curFile, long initChannelPosition, long flyingThreshold)
				throws IOException {
			this.curFile = curFile;
			this.flyingThreshhold = flyingThreshold;
			gate = new Gate(simpleDesc());
			curPosition = extractStartOffset(curFile) + initChannelPosition;
			referenceFileChannel = new ReferenceFileChannel(new DefaultControllableFile(curFile), initChannelPosition);
		}

		@Override
		public void close() throws IOException {

			readers.remove(this);
			referenceFileChannel.close();
		}

		@Override
		public ReferenceFileRegion read() throws IOException {
			try {
				gate.tryPass();
				offsetNotifier.await(curPosition);
				readNextFileIfNecessary();
			} catch (InterruptedException e) {
				logger.info("[read]", e);
				Thread.currentThread().interrupt();
			}

			ReferenceFileRegion referenceFileRegion = referenceFileChannel.readTilEnd();

			curPosition += referenceFileRegion.count();
			
			referenceFileRegion.setTotalPos(curPosition);
			
			if (referenceFileRegion.count() < 0) {
				logger.error("[read]{}", referenceFileRegion);
			}

			checkCloseGate(flying.incrementAndGet());
			return referenceFileRegion;
		}

		private void checkCloseGate(long current) {

			debugPrint(current);

			if(gate.isOpen() && (current >= flyingThreshhold)){
				logger.info("[increaseFlying][close gate]{}, {}", gate, current);
				gate.close();
				//just in case, before gate.close(), all flushed
				checkOpenGate(flying.get());
			}
		}

		private void debugPrint(long current) {

			if(logger.isDebugEnabled() && (current > 4)){
				int intCurrent = (int) current;
				if((intCurrent & (intCurrent-1)) == 0){
					logger.debug("[flying]{}, {}", gate, current);
				}
			}
		}

		private void checkOpenGate(long current){

			debugPrint(current);

			if(!gate.isOpen() && (current <= (flyingThreshhold >> 2))){
				logger.info("[decreaseFlying][open gate]{}, {}", gate, flying);
				gate.open();
			}
		}

		@Override
		public void flushed(ReferenceFileRegion referenceFileRegion){
			checkOpenGate(flying.decrementAndGet());
		}

		private void readNextFileIfNecessary() throws IOException {

			makeSureOpen();

			if (!referenceFileChannel.hasAnythingToRead()) {
				// TODO notify when next file ready
				File nextFile = findNextFile(curFile);
				if (nextFile != null) {
					curFile = nextFile;
					referenceFileChannel.close();
					referenceFileChannel = new ReferenceFileChannel(new DefaultControllableFile(curFile));
				}
			}
		}

		public File getCurFile() {
			return curFile;
		}

		@Override
		public String toString() {
			return "curFile:" + curFile;
		}

	}

	@Override
	public boolean awaitCommandsOffset(long offset, int timeMilli) throws InterruptedException {
		return offsetNotifier.await(offset, timeMilli);
	}

	@Override
	public long lowestReadingOffset() {
		long lowestReadingOffset = Long.MAX_VALUE;

		for (DefaultCommandReader reader : readers.keySet()) {
			File readingFile = reader.getCurFile();
			if (readingFile != null) {
				lowestReadingOffset = Math.min(lowestReadingOffset, extractStartOffset(readingFile));
			}
		}

		return lowestReadingOffset;
	}

	@Override
	public void addCommandsListener(long offset, final CommandsListener listener) throws IOException {

		makeSureOpen();
		logger.info("[addCommandsListener][begin] from offset {}, {}", offset, listener);

		CommandReader cmdReader = null;

		try {
			cmdReader = beginRead(offset);
		} finally {
			// ensure beforeCommand() is always called
			listener.beforeCommand();
		}

		logger.info("[addCommandsListener] from offset {}, {}", offset, cmdReader);

		try {
			while (listener.isOpen() && !Thread.currentThread().isInterrupted()) {

				final ReferenceFileRegion referenceFileRegion = cmdReader.read();

				logger.debug("[addCommandsListener] {}", referenceFileRegion);

				if(delayTraceLogger.isDebugEnabled()){
					delayTraceLogger.debug("[write][begin]{}, {}", listener, referenceFileRegion.getTotalPos());
				}
				commandStoreDelay.beginSend(listener, referenceFileRegion.getTotalPos());
				
				ChannelFuture future = listener.onCommand(referenceFileRegion);
						
				if(future != null){
					CommandReader finalCmdReader = cmdReader;
					future.addListener(new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) throws Exception {

							finalCmdReader.flushed(referenceFileRegion);
							commandStoreDelay.flushSucceed(listener, referenceFileRegion.getTotalPos());
							if(logger.isDebugEnabled()){
								delayTraceLogger.debug("[write][ end ]{}, {}", listener, referenceFileRegion.getTotalPos());
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
		logger.info("[addCommandsListener][end] from offset {}, {}", offset, listener);
	}

	@Override
	public void close() throws IOException {

		if(cmpAndSetClosed()){
			logger.info("[close]{}", this);
			CommandFileContext commandFileContext = cmdFileCtxRef.get();
			if (commandFileContext != null) {
				commandFileContext.close();
			}
		}else{
			logger.warn("[close][already closed]{}", this);
		}
	}

	@Override
	public void destroy() throws Exception {
		
		logger.info("[destroy]{}", this);
		File [] files = allFiles();
		if(files != null){
			for(File file : files){
				boolean result = file.delete();
				logger.info("[destroy][delete file]{}, {}", file, result);
			}
		}
	}
	
	@Override
	public String toString() {
		return String.format("CommandStore:%s", baseDir);
	}

	public String simpleDesc(){

		File desc1 = baseDir.getParentFile();
		File desc2 = null;
		if(desc1 != null){
			desc2 = desc1.getParentFile();
		}
		return String.format("%s.%s",
				desc2 == null?null:desc2.getName(),
				desc1 == null?null:desc1.getName());
	}

	@Override
	public long lowestAvailableOffset() {
		
		long minCmdOffset = Long.MAX_VALUE; // start from zero
		File[] files = allFiles();

		if (files == null || files.length == 0) {
			logger.info("[minCmdKeeperOffset][no cmd files][start offset 0]");
			minCmdOffset = 0L;
		} else {
			for (File cmdFile : files) {
				minCmdOffset = Math.min(extractStartOffset(cmdFile), minCmdOffset);
			}
		}
		return minCmdOffset;
	}

	@Override
	public boolean retainCommands(CommandsGuarantee commandsGuarantee) {
		try {
			gcLock.lock();
			long needCmdOffset = commandsGuarantee.getNeededCommandOffset();
			long minOffset = lowestAvailableOffset();
			if (minOffset <= needCmdOffset) {
				this.commandsGuarantees.add(commandsGuarantee);
				return true;
			}
		} finally {
			gcLock.unlock();
		}

		return false;
	}

	private void timeoutGuarantees() {
		List<CommandsGuarantee> timeoutGuarantees = commandsGuarantees.stream().filter(CommandsGuarantee::isTimeout).collect(Collectors.toList());
		commandsGuarantees.removeAll(timeoutGuarantees);
	}

	private void finishGuarantees() {
		List<CommandsGuarantee> finishGuarantees = commandsGuarantees.stream().filter(CommandsGuarantee::isFinish).collect(Collectors.toList());
		commandsGuarantees.removeAll(finishGuarantees);
	}

	private long minGuaranteeOffset() {
		long minOffset = Long.MAX_VALUE;
		for (CommandsGuarantee commandsGuarantee : commandsGuarantees) {
			long offset = commandsGuarantee.getNeededCommandOffset();
			minOffset = Long.min(offset, minOffset);
		}

		return minOffset;
	}

	@Override
	public long getCommandsLastUpdatedAt() {
		return cmdFileCtxRef.get().getLastModified();
	}

	@Override
	public void gc() {
		try {
			gcLock.lock();
			timeoutGuarantees();
			finishGuarantees();

			for (File cmdFile : allFiles()) {
				long fileStartOffset = extractStartOffset(cmdFile);
				if (canDeleteCmdFile(Long.min(lowestReadingOffset(), minGuaranteeOffset()), fileStartOffset, cmdFile.length(),
						cmdFile.lastModified())) {
					logger.info("[GC] delete command file {}", cmdFile);
					cmdFile.delete();
				}
			}
		} finally {
			gcLock.unlock();
		}
	}
	
	protected boolean canDeleteCmdFile(long lowestReadingOffset, long fileStartOffset, long fileSize, long lastModified) {
		
		boolean lowestReading = (fileStartOffset + fileSize < lowestReadingOffset);
		
		logger.debug("[canDeleteCmdFile][lowestReading]{}, {}+{}<{}", lowestReading, fileStartOffset, fileSize, lowestReadingOffset);
		if(!lowestReading){
			return false;
		}

		Date now = new Date();
		long maxMilliKeepCmd = TimeUnit.SECONDS.toMillis(maxTimeSecondKeeperCmdFileAfterModified.getAsInt());
		boolean time = now.getTime() - lastModified >= minTimeMilliToGcAfterModified;
		boolean fresh = now.getTime() - lastModified <= maxMilliKeepCmd;

		logger.debug("[canDeleteCmdFile][time]{}, {} - {} > {}", time, now, new Date(lastModified), minTimeMilliToGcAfterModified);
		if(!time){
			return false;
		}
		logger.debug("[canDeleteCmdFile][fresh]{}, {} - {} < {}", fresh, now, new Date(lastModified), maxMilliKeepCmd);
		if (!fresh) {
			return true;
		}

		long totalLength = totalLength();
		long totalKeep = (long)fileSize * fileNumToKeep.getAsInt();
		boolean fileKeep = totalLength - (fileStartOffset + fileSize) > totalKeep;
		
		logger.debug("[canDeleteCmdFile][fileKeep]{}, {} - {} > {}({}*{})", fileKeep, totalLength, (fileStartOffset + fileSize), totalKeep, fileSize, fileNumToKeep);
		if(!fileKeep){
			return false;
		}
		return true;
	}
	
}
