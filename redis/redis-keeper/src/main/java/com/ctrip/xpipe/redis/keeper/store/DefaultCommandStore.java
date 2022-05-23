package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.monitor.CommandStoreDelay;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.store.cmd.OffsetReplicationProgress;
import com.ctrip.xpipe.redis.keeper.util.KeeperLogger;
import com.ctrip.xpipe.utils.FileUtils;
import com.ctrip.xpipe.utils.OffsetNotifier;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

/**
 * @author qing.gu
 *
 *         Aug 9, 2016
 */
public class DefaultCommandStore extends AbstractStore implements CommandStore<OffsetReplicationProgress, ReferenceFileRegion> {

	private final static Logger logger = LoggerFactory.getLogger(DefaultCommandStore.class);
	
	private final static Logger delayTraceLogger = KeeperLogger.getDelayTraceLog();

	public static final long DEFAULT_COMMAND_READER_FLYING_THRESHOLD = 1 << 15;

	private final File baseDir;

	private final String fileNamePrefix;

	private final int maxFileSize;
	
	private final IntSupplier fileNumToKeep;
	private final int minTimeMilliToGcAfterModified;

	private final IntSupplier maxTimeSecondKeeperCmdFileAfterModified;

	private final FilenameFilter cmdFileFilter;

	private final FilenameFilter idxFileFilter;

	private final FilenameFilter allFileFilter;

	private final ConcurrentMap<CommandReader, Boolean> readers = new ConcurrentHashMap<>();

	private final OffsetNotifier offsetNotifier;

	private final long commandReaderFlyingThreshold;

	private CommandStoreDelay commandStoreDelay;

	private List<CommandsGuarantee> commandsGuarantees = new CopyOnWriteArrayList<>();

	private ReentrantLock gcLock = new ReentrantLock();

	private CommandReaderWriterFactory<OffsetReplicationProgress, ReferenceFileRegion> cmdReaderWriterFactory;

	private CommandWriter cmdWriter;

	private List<CommandFileOffsetGtidIndex> cmdIndexList = new CopyOnWriteArrayList<>();

	private static final String INDEX_FILE_PREFIX = "idx_";

	public DefaultCommandStore(File file, int maxFileSize, CommandReaderWriterFactory<OffsetReplicationProgress, ReferenceFileRegion> cmdReaderWriterFactory, KeeperMonitor keeperMonitor) throws IOException {
		this(file, maxFileSize, () -> 12 * 3600, 3600*1000, () -> 20, DEFAULT_COMMAND_READER_FLYING_THRESHOLD, cmdReaderWriterFactory, keeperMonitor);
	}

	public DefaultCommandStore(File file, int maxFileSize, IntSupplier maxTimeSecondKeeperCmdFileAfterModified,
							   int minTimeMilliToGcAfterModified, IntSupplier fileNumToKeep,
							   long commandReaderFlyingThreshold, CommandReaderWriterFactory<OffsetReplicationProgress, ReferenceFileRegion> cmdReaderWriterFactory,
							   KeeperMonitor keeperMonitor) throws IOException {
		
		this.baseDir = file.getParentFile();
		this.fileNamePrefix = file.getName();
		this.maxFileSize = maxFileSize;
		this.maxTimeSecondKeeperCmdFileAfterModified = maxTimeSecondKeeperCmdFileAfterModified;
		this.fileNumToKeep = fileNumToKeep;
		this.commandReaderFlyingThreshold = commandReaderFlyingThreshold;
		this.minTimeMilliToGcAfterModified = minTimeMilliToGcAfterModified;
		this.cmdReaderWriterFactory = cmdReaderWriterFactory;
		this.commandStoreDelay = keeperMonitor.createCommandStoreDelay(this);

		cmdFileFilter = new PrefixFileFilter(fileNamePrefix);
		idxFileFilter = new PrefixFileFilter(INDEX_FILE_PREFIX + fileNamePrefix);
		allFileFilter = new PrefixFileFilter(new String[] {fileNamePrefix, INDEX_FILE_PREFIX + fileNamePrefix});

		cmdWriter = cmdReaderWriterFactory.createCmdWriter(this, maxFileSize, delayTraceLogger);
		cmdWriter.initialize();

		offsetNotifier = new OffsetNotifier(cmdWriter.totalLength() - 1);
		intiCmdFileIndex();
	}

	private void intiCmdFileIndex() {
		File[] files = allIndexFiles();
		List<CommandFileOffsetGtidIndex> localIndexList = new LinkedList<>();
		for (File idxFile: files) {
			String cmdFileName = idxFile.getName().substring(INDEX_FILE_PREFIX.length());
			File file = new File(baseDir, cmdFileName);
			if (!file.exists()) {
				logger.info("[intiCmdFileIndex][{}] skip for no cmd file", idxFile);
				continue;
			}

			long startOffset = extractStartOffset(file);
			CommandFile commandFile = new CommandFile(file, startOffset);
			FileUtils.readFileAsStringLineByLine(idxFile, idxStr -> {
				CommandFileOffsetGtidIndex idx = CommandFileOffsetGtidIndex.createFromRawString(idxStr, commandFile);
				if (null != idx) localIndexList.add(idx);
			});
		}

		Collections.sort(localIndexList);
		this.cmdIndexList.addAll(localIndexList);
	}

	private File fileForStartOffset(long startOffset) {
		return new File(baseDir, fileNamePrefix + startOffset);
	}

	private long findMaxStartOffset() {
		
		long maxStartOffset = 0;
		File[] files = allCmdFiles();
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

	@Override
	public File findIndexFile(CommandFile commandFile) {
		if (!commandFile.getFile().exists()) throw new IllegalArgumentException("command file must exist " + commandFile);
		return new File(baseDir, INDEX_FILE_PREFIX + commandFile.getFile().getName());
	}

	@Override
	public void addIndex(CommandFileOffsetGtidIndex index) {
		this.cmdIndexList.add(index);
	}

	private File[] allCmdFiles() {
		File []files = baseDir.listFiles(cmdFileFilter);
		if(files == null){
			files = new File[0];
		}
		return files;
	}

	private File[] allIndexFiles() {
		File []files = baseDir.listFiles(idxFileFilter);
		if(files == null){
			files = new File[0];
		}
		return files;
	}

	private File[] allFiles() {
		File []files = baseDir.listFiles(allFileFilter);
		if(files == null){
			files = new File[0];
		}
		return files;
	}

	private boolean delCmdFile(File cmdFile) {
		File idxFile = new File(baseDir, INDEX_FILE_PREFIX + cmdFile.getName());
		if (idxFile.exists()) {
			if (!idxFile.delete()) {
				logger.warn("[delCmdFile][{}] del idx file fail", idxFile);
			}

			this.cmdIndexList = cmdIndexList.stream()
					.filter(index -> !index.getCommandFile().getFile().equals(cmdFile))
					.collect(Collectors.toCollection(CopyOnWriteArrayList::new));
		}

		return cmdFile.delete();
	}

	private long extractStartOffset(File file) {
		return Long.parseLong(file.getName().substring(fileNamePrefix.length()));
	}

	@Override
	public int appendCommands(ByteBuf byteBuf) throws IOException {
		
		makeSureOpen();

		cmdWriter.rotateFileIfNecessary();

		commandStoreDelay.beginWrite();
		
		int wrote = cmdWriter.write(byteBuf);

		long offset = cmdWriter.totalLength() - 1;
		commandStoreDelay.endWrite(offset);

		offsetNotifier.offsetIncreased(offset);
		
		return wrote;
	}

	@Override
	public long totalLength() {
		return cmdWriter.totalLength();
	}

	public CommandReader<ReferenceFileRegion> beginRead(OffsetReplicationProgress replicationProgress) throws IOException {

		makeSureOpen();

		CommandReader<ReferenceFileRegion> reader = cmdReaderWriterFactory.createCmdReader(replicationProgress, this, offsetNotifier, commandReaderFlyingThreshold);
		readers.put(reader, Boolean.TRUE);
		return reader;
	}

	public void rotateFileIfNecessary() throws IOException {
		cmdWriter.rotateFileIfNecessary();
	}

	public CommandFile findFileForOffset(long targetStartOffset) throws IOException {
		File[] files = baseDir.listFiles(cmdFileFilter);
		if (files != null) {
			for (File file : files) {
				long startOffset = extractStartOffset(file);
				if (targetStartOffset >= startOffset && (targetStartOffset < startOffset + file.length()
						|| targetStartOffset < startOffset + maxFileSize)) {
					return new CommandFile(file, startOffset);
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

	@Override
	public CommandFile findLatestFile() throws IOException {
		long maxStartOffset = findMaxStartOffset();
		return new CommandFile(fileForStartOffset(maxStartOffset), maxStartOffset);
	}

	@Override
	public CommandFileSegment findLastFileSegment() {
		if (this.cmdIndexList.isEmpty()) {
			// TODO: return cmd_0 with rdb gtidset or do fsync
			throw new IllegalArgumentException();
		} else {
			CommandFileOffsetGtidIndex lastIndex = this.cmdIndexList.get(cmdIndexList.size() - 1);
			return new CommandFileSegment(lastIndex);
		}
	}

	public CommandFileSegment findFirstFileSegment(GtidSet excludedGtidSet) {
		makeSureOpen();

		if (cmdIndexList.isEmpty()) {
			// TODO: return cmd_0 with rdb gtidset or do fsync
			throw new IllegalArgumentException();
		}

		Set<String> interestedSrcIds = excludedGtidSet.getUUIDs();
		Iterator<CommandFileOffsetGtidIndex> indexIterator = cmdIndexList.iterator();
		CommandFileOffsetGtidIndex startIndex = indexIterator.next();
		CommandFileOffsetGtidIndex endIndex = null;

		GtidSet storeExcludedGtidSet = startIndex.getExcludedGtidSet().filterGtid(interestedSrcIds);
		if (!storeExcludedGtidSet.isContainedWithin(excludedGtidSet)) {
			// TODO: need handle fsync
			throw new IllegalArgumentException();
		}

		while (indexIterator.hasNext()) {
			CommandFileOffsetGtidIndex index = indexIterator.next();
			GtidSet includedGtidSet = index.getExcludedGtidSet().filterGtid(interestedSrcIds)
					.subtract(startIndex.getExcludedGtidSet());
			if (!includedGtidSet.isContainedWithin(excludedGtidSet)) {
				endIndex = index;
				if (!indexIterator.hasNext()) {
					// right bound open
					endIndex = null;
				}
			} else if (null == endIndex) {
				startIndex = index;
			} else {
				break;
			}
		}

		return new CommandFileSegment(startIndex, endIndex);
	}

	public CommandFile findNextFile(File curFile) {
		if (!curFile.getParentFile().equals(baseDir)) {
			throw new IllegalArgumentException("file " + curFile + "not in dir " + baseDir);
		}

		long startOffset = extractStartOffset(curFile);
		File nextFile = fileForStartOffset(startOffset + curFile.length());
		if (nextFile.isFile()) {
			return new CommandFile(nextFile, extractStartOffset(nextFile));
		} else {
			return null;
		}
	}

	@Override
	public boolean awaitCommandsOffset(long offset, int timeMilli) throws InterruptedException {
		return offsetNotifier.await(offset, timeMilli);
	}

	@Override
	public long lowestReadingOffset() {
		long lowestReadingOffset = Long.MAX_VALUE;

		for (CommandReader reader : readers.keySet()) {
			File readingFile = reader.getCurFile();
			if (readingFile != null) {
				lowestReadingOffset = Math.min(lowestReadingOffset, extractStartOffset(readingFile));
			}
		}

		return lowestReadingOffset;
	}

	@Override
	public void addCommandsListener(OffsetReplicationProgress progress, final CommandsListener listener) throws IOException {

		makeSureOpen();
		logger.info("[addCommandsListener][begin] from offset {}, {}", progress, listener);

		CommandReader<ReferenceFileRegion> cmdReader = null;

		try {
			cmdReader = beginRead(progress);
		} finally {
			// ensure beforeCommand() is always called
			listener.beforeCommand();
		}

		logger.info("[addCommandsListener] from {}, {}", progress, cmdReader);

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
					CommandReader<ReferenceFileRegion> finalCmdReader = cmdReader;
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
		logger.info("[addCommandsListener][end] from {}, {}", progress, listener);
	}

	@Override
	public CommandFile newCommandFile(long startOffset) throws IOException {
		makeSureOpen();

		CommandFile commandFile = findFileForOffset(startOffset);
		if (null != commandFile) return commandFile;

		File newFile = new File(baseDir, fileNamePrefix + startOffset);
		return new CommandFile(newFile, startOffset);
	}

	@Override
	public void addReader(CommandReader reader) {
		this.readers.put(reader, Boolean.TRUE);
	}

	@Override
	public void removeReader(CommandReader reader) {
		this.readers.remove(reader);
	}

	@Override
	public void close() throws IOException {

		if(cmpAndSetClosed()){
			logger.info("[close]{}", this);
			cmdWriter.close();
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
		File[] files = allCmdFiles();

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
		return cmdWriter.getFileLastModified();
	}

	@Override
	public void gc() {
		try {
			gcLock.lock();
			timeoutGuarantees();
			finishGuarantees();

			for (File cmdFile : allCmdFiles()) {
				long fileStartOffset = extractStartOffset(cmdFile);
				if (canDeleteCmdFile(Long.min(lowestReadingOffset(), minGuaranteeOffset()), fileStartOffset, cmdFile.length(),
						cmdFile.lastModified())) {
					logger.info("[GC] delete command file {}", cmdFile);
					delCmdFile(cmdFile);
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

	@VisibleForTesting
	protected List<CommandFileOffsetGtidIndex> getIndexList() {
		return cmdIndexList;
	}
	
}
