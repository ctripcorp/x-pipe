package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.exception.replication.UnexpectedReplIdException;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.store.cmd.OffsetCommandReaderWriterFactory;
import com.ctrip.xpipe.redis.core.store.OffsetReplicationProgress;
import com.ctrip.xpipe.redis.keeper.store.meta.DefaultMetaStore;
import com.ctrip.xpipe.utils.FileUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

// TODO make methods correctly sequenced
public class DefaultReplicationStore extends AbstractStore implements ReplicationStore {

	private final static Logger logger = LoggerFactory.getLogger(DefaultReplicationStore.class);

	private final static FileFilter RDB_FILE_FILTER = new FileFilter() {

		@Override
		public boolean accept(File path) {
			return path.isFile() && path.getName().startsWith("rdb_");
		}
	};

	private File baseDir;

	private AtomicReference<RdbStore> rdbStoreRef = new AtomicReference<>();

	private AtomicReference<RdbStore> rordbStoreRef = new AtomicReference<>();

	private ConcurrentMap<RdbStore, Boolean> previousRdbStores = new ConcurrentHashMap<>();

	protected CommandStore cmdStore;

	protected MetaStore metaStore;

	private int cmdFileSize;

	private KeeperConfig config;

	protected Object lock = new Object();

	private AtomicInteger rdbUpdateCount = new AtomicInteger();

	private KeeperMonitor keeperMonitor;

	public static final String KEY_CMD_RETAIN_TIMEOUT_MILLI = "commandsRetainTimeoutMilli";

	protected int commandsRetainTimeoutMilli = Integer.parseInt(System.getProperty(KEY_CMD_RETAIN_TIMEOUT_MILLI, "1800000"));

	private CommandReaderWriterFactory cmdReaderWriterFactory;

	protected SyncRateManager syncRateManager;

	public DefaultReplicationStore(File baseDir, KeeperConfig config, String keeperRunid,
								   KeeperMonitor keeperMonitor, SyncRateManager syncRateManager) throws IOException {
		this(baseDir, config, keeperRunid, new OffsetCommandReaderWriterFactory(), keeperMonitor, syncRateManager);
	}

	public DefaultReplicationStore(File baseDir, KeeperConfig config, String keeperRunid,
								   CommandReaderWriterFactory cmdReaderWriterFactory,
								   KeeperMonitor keeperMonitor, SyncRateManager syncRateManager) throws IOException {
		this.baseDir = baseDir;
		this.cmdFileSize = config.getReplicationStoreCommandFileSize();
		this.config = config;
		this.keeperMonitor = keeperMonitor;
		this.cmdReaderWriterFactory = cmdReaderWriterFactory;
		this.syncRateManager = syncRateManager;

		metaStore = new DefaultMetaStore(baseDir, keeperRunid);

		ReplicationStoreMeta meta = metaStore.dupReplicationStoreMeta();

		if (meta != null && meta.getRdbFile() != null) {
			File rdb = new File(baseDir, meta.getRdbFile());
			if (rdb.isFile()) {
				rdbStoreRef.set(createRdbStore(rdb, meta.getReplId(), meta.getRdbLastOffset(), initRdbEofType(meta)));
				rdbStoreRef.get().updateRdbType(RdbStore.Type.NORMAL);
				rdbStoreRef.get().updateRdbGtidSet(null != meta.getRdbGtidSet() ? meta.getRdbGtidSet() : GtidSet.EMPTY_GTIDSET);
			}
		}
		if (meta != null && meta.getRordbFile() != null) {
			File rordb = new File(baseDir, meta.getRordbFile());
			if (rordb.isFile()) {
				rordbStoreRef.set(createRdbStore(rordb, meta.getReplId(), meta.getRordbLastOffset(), initRordbEofType(meta)));
				rordbStoreRef.get().updateRdbType(RdbStore.Type.RORDB);
				rordbStoreRef.get().updateRdbGtidSet(null != meta.getRordbGtidSet() ? meta.getRordbGtidSet() : GtidSet.EMPTY_GTIDSET);
			}
		}
		if (null != meta && null != meta.getCmdFilePrefix()) {
			cmdStore = createCommandStore(baseDir, meta, cmdFileSize, config, cmdReaderWriterFactory, keeperMonitor);
			if (meta.getRdbLastOffset() != null) {
				cmdStore.setBaseIndex(meta.getRdbGtidSet(), meta.getRdbLastOffset() - (meta.getBeginOffset() - 1));
			} else if (meta.getRordbLastOffset() != null) {
				cmdStore.setBaseIndex(meta.getRordbGtidSet(), meta.getRordbLastOffset() - (meta.getBeginOffset() - 1));
			}
		}

		removeUnusedRdbFiles();
	}

	protected EofType initRdbEofType(ReplicationStoreMeta meta) {

		// must has length field
		getLogger().info("[initRdbEofType][leneof]{}", meta);
		return new LenEofType(meta.getRdbFileSize());
	}

	protected EofType initRordbEofType(ReplicationStoreMeta meta) {

		// must has length field
		getLogger().info("[initRordbEofType][leneof]{}", meta);
		return new LenEofType(meta.getRordbFileSize());
	}

	private void removeUnusedRdbFiles() {

		@SuppressWarnings("resource")
		RdbStore rdbStore = rdbStoreRef.get();
		RdbStore rordbStore = rordbStoreRef.get();

		for (File rdbFile : rdbFilesOnFS()) {
			if (rdbStore != null && rdbStore.sameRdbFile(rdbFile)) continue;
			if (rordbStore != null && rordbStore.sameRdbFile(rdbFile)) continue;

			getLogger().info("[removeUnusedRdbFile] {}", rdbFile);
			rdbFile.delete();
		}
	}

	@Override
	public RdbStore prepareRdb(String replId, long rdbOffset, EofType eofType) throws IOException {
		makeSureOpen();
		baseDir.mkdirs();

		getLogger().info("[makeRdb] replId:{}, rdbOffset:{}, eof:{}", replId, rdbOffset, eofType);
		String rdbFile = newRdbFileName();
		return createRdbStore(new File(baseDir, rdbFile), replId, rdbOffset, eofType);
	}

	@Override
	public void checkReplId(String expectReplId) {
		String currentReplId = metaStore.getReplId();
		if (!Objects.equals(expectReplId, currentReplId)) {
			throw new UnexpectedReplIdException(expectReplId, currentReplId);
		}
	}

	public void confirmRdb(RdbStore rdbStore) throws IOException {
		makeSureOpen();

		getLogger().info("[confirmRdb] type:{}, replId:{}, rdbOffset:{}, eof:{}",
				rdbStore.getRdbType(), rdbStore.getReplId(), rdbStore.getRdbOffset(), rdbStore.getEofType());
		AtomicReference<RdbStore> storeRef = RdbStore.Type.NORMAL.equals(rdbStore.getRdbType()) ? rdbStoreRef : rordbStoreRef;

		String cmdFilePrefix = "cmd_" + UUID.randomUUID().toString() + "_";
		ReplicationStoreMeta newMeta = metaStore.rdbConfirm(rdbStore.getReplId(), rdbStore.getRdbOffset() + 1,
				rdbStore.getGtidSet(), rdbStore.getRdbFile().getName(), rdbStore.getRdbType(), rdbStore.getEofType(), cmdFilePrefix);

		rdbStore.addListener(createRdbStoreListener(rdbStore));
		storeRef.set(rdbStore);
		cmdStore = createCommandStore(baseDir, newMeta, cmdFileSize, config, cmdReaderWriterFactory, keeperMonitor);
		cmdStore.setBaseIndex(rdbStore.getGtidSet(), rdbStore.rdbOffset() - (newMeta.getBeginOffset() - 1));
	}

	@Override
	public void checkReplIdAndUpdateRdb(RdbStore rdbStore) throws IOException {
		makeSureOpen();

		synchronized (lock) {
			rdbUpdateCount.incrementAndGet();

			File dumpedRdbFile = rdbStore.getRdbFile();
			if (!baseDir.equals(dumpedRdbFile.getParentFile())) {
				throw new IllegalStateException("update rdb error, filePath:" + dumpedRdbFile.getAbsolutePath()
						+ ", baseDir:" + baseDir.getAbsolutePath());
			}
			EofType eofType = rdbStore.getEofType();
			long rdbOffset = rdbStore.rdbOffset();
			RdbStore.Type rdbType = rdbStore.getRdbType();
			String gtidSet = rdbStore.getGtidSet();
			AtomicReference<RdbStore> storeRef = RdbStore.Type.NORMAL.equals(rdbType) ? rdbStoreRef : rordbStoreRef;

			@SuppressWarnings("unused")
			ReplicationStoreMeta metaDup = metaStore.checkReplIdAndUpdateRdbInfo(dumpedRdbFile.getName(),
					rdbType, eofType, rdbOffset, gtidSet, rdbStore.getReplId());

			rdbStore.addListener(createRdbStoreListener(rdbStore));

			getLogger().info("[checkReplIdAndUpdateRdb] new file:{}, type:{} eofType:{}, rdbOffset:{}", dumpedRdbFile, rdbType, eofType, rdbOffset);
			RdbStore oldRdbStore = storeRef.get();
			storeRef.set(rdbStore);
			if (null!= oldRdbStore) previousRdbStores.put(oldRdbStore, Boolean.TRUE);
			cmdStore.setBaseIndex(rdbStore.getGtidSet(), rdbStore.rdbOffset() - (metaDup.getBeginOffset() - 1));
		}
	}

	@Override
	public void continueFromOffset(String replId, long continueOffset) throws IOException{
		makeSureOpen();

		getLogger().info("[continueFromOffset] {}:{}", replId, continueOffset);
		baseDir.mkdirs();

		String cmdFilePrefix = "cmd_" + UUID.randomUUID().toString() + "_";
		ReplicationStoreMeta newMeta = metaStore.continueFromOffset(replId, continueOffset, cmdFilePrefix);

		cmdStore = createCommandStore(baseDir, newMeta, cmdFileSize, config, cmdReaderWriterFactory, keeperMonitor);
	}

	protected CommandStore createCommandStore(File baseDir, ReplicationStoreMeta replMeta, int cmdFileSize,
											  KeeperConfig config, CommandReaderWriterFactory cmdReaderWriterFactory,
											  KeeperMonitor keeperMonitor) throws IOException {
		DefaultCommandStore cmdStore = new DefaultCommandStore(new File(baseDir, replMeta.getCmdFilePrefix()), cmdFileSize,
				config::getReplicationStoreCommandFileKeepTimeSeconds,
				config.getReplicationStoreMinTimeMilliToGcAfterCreate(),
				config::getReplicationStoreCommandFileNumToKeep,
				config.getCommandReaderFlyingThreshold(),
				cmdReaderWriterFactory, keeperMonitor);
		cmdStore.attachRateLimiter(syncRateManager.generatePsyncRateLimiter());
		try {
			cmdStore.initialize();
		} catch (Exception e) {
			logger.info("[createCommandStore] init fail", e);
			throw new XpipeRuntimeException("cmdStore init fail", e);
		}

		return cmdStore;
	}

	protected RdbStore createRdbStore(File rdb, String replId, long rdbOffset, EofType eofType) throws IOException {
		RdbStore rdbStore = new DefaultRdbStore(rdb, replId, rdbOffset, eofType);
		rdbStore.attachRateLimiter(syncRateManager.generateFsyncRateLimiter());
		return rdbStore;
	}

	protected RdbStoreListener createRdbStoreListener(RdbStore rdbStore) {
		return new ReplicationStoreRdbFileListener(rdbStore);
	}

	@Override
	public DumpedRdbStore prepareNewRdb() throws IOException {
		makeSureOpen();

		DumpedRdbStore rdbStore = new DefaultDumpedRdbStore(new File(baseDir, newRdbFileName()));
		rdbStore.attachRateLimiter(syncRateManager.generateFsyncRateLimiter());
		return rdbStore;
	}

	public RdbStore getRdbStore() {
		return rdbStoreRef.get();
	}

	@Override
	public long beginOffsetWhenCreated() {

		if(metaStore == null || metaStore.beginOffset() == null){
			throw new IllegalStateException("meta store null:" + this);
		}
		return metaStore.beginOffset();
	}

	@Override
	public long getEndOffset() {
		if (metaStore == null || metaStore.beginOffset() == null || cmdStore == null) {
			// TODO
			return ReplicationStoreMeta.DEFAULT_END_OFFSET;
		} else {
			long beginOffset = metaStore.beginOffset();
			long totalLength = cmdStore.totalLength();

			getLogger().debug("[getEndOffset]B:{}, L:{}", beginOffset, totalLength);
			return beginOffset + totalLength - 1;
		}
	}

	protected long beginOffset() {
		return metaStore == null? ReplicationStoreMeta.DEFAULT_BEGIN_OFFSET : metaStore.beginOffset();
	}

	@Override
	public long firstAvailableOffset() {

		long beginOffset = beginOffset();

		long minCmdOffset = cmdStore == null ? 0 : cmdStore.lowestAvailableOffset();
		long firstAvailableOffset = minCmdOffset + beginOffset;
		return firstAvailableOffset;
	}

	@Override
	public MetaStore getMetaStore() {
		return metaStore;
	}

	@Override
	public void shiftReplicationId(String newReplId) throws IOException {

		getLogger().info("[shiftReplicationId]{}", newReplId);
		if(newReplId != null){
			this.metaStore.shiftReplicationId(newReplId, getEndOffset());
		}
	}

	@Override
	public long lastReplDataUpdatedAt() {
		if (null != cmdStore) {
			return cmdStore.getCommandsLastUpdatedAt();
		}

		RdbStore rdbStore = rdbStoreRef.get();
		if (null != rdbStore) {
			return rdbStore.getRdbFileLastModified();
		}

		RdbStore rordbStore = rordbStoreRef.get();
		if (null != rordbStore) {
			return rordbStore.getRdbFileLastModified();
		}

		return 0L;
	}

	private File[] rdbFilesOnFS() {
		File[] rdbFiles = baseDir.listFiles(RDB_FILE_FILTER);
		return rdbFiles != null ? rdbFiles : new File[0];
	}

	protected FullSyncContext lockAndCheckIfRordbFullSyncPossible() {
		synchronized (lock) {
			getLogger().info("[lockAndCheckIfRordbFullSyncPossible]");
			return lockAndCheckIfFullSyncPossible(rordbStoreRef.get());
		}
	}

	protected FullSyncContext lockAndCheckIfRdbFullSyncPossible() {
		synchronized (lock) {
			getLogger().info("[lockAndCheckIfRdbFullSyncPossible]");
			return lockAndCheckIfFullSyncPossible(rdbStoreRef.get());
		}
	}

	protected FullSyncContext lockAndCheckIfFullSyncPossible(RdbStore rdbStore) {
		if (rdbStore == null || !rdbStore.checkOk()) {
			getLogger().info("[lockAndCheckIfFullSyncPossible][false]{}", rdbStore);
			return new FullSyncContext(false, FULLSYNC_FAIL_CAUSE.RDB_NOT_OK);
		}

		rdbStore.incrementRefCount();
		long rdbOffset = rdbStore.rdbOffset();
		long minOffset = firstAvailableOffset();
		long maxOffset = getEndOffset();

		/**
		 * rdb and cmd is continuous AND not so much cmd after rdb
		 */
		long cmdAfterRdbThreshold = config.getReplicationStoreMaxCommandsToTransferBeforeCreateRdb();
		FullSyncContext context;
		if (minOffset > rdbOffset + 1) {
			getLogger().info("minOffset > rdbOffset + 1");
			getLogger().info("[isFullSyncPossible][false][miss cmd after rdb] {} <= {} + 1", minOffset, rdbOffset);
			context = new FullSyncContext(false, FULLSYNC_FAIL_CAUSE.MISS_CMD_AFTER_RDB);
		} else if (maxOffset - rdbOffset > cmdAfterRdbThreshold) {
			getLogger().info("maxOffset - rdbOffset > cmdAfterRdbThreshold");
			getLogger().info("[isFullSyncPossible][false][too much cmd after rdb] {} - {} <= {}",
					maxOffset, rdbOffset, cmdAfterRdbThreshold);
			context = new FullSyncContext(false, FULLSYNC_FAIL_CAUSE.TOO_MUCH_CMD_AFTER_RDB);
		} else {
			getLogger().info("minOffset <= rdbOffset + 1 && maxOffset - rdbOffset <= cmdAfterRdbThreshold");
			getLogger().info("[isFullSyncPossible][true] {} <= {} + 1 && {} - {} <= {}",
					minOffset, rdbOffset, maxOffset, rdbOffset, cmdAfterRdbThreshold);
			context = new FullSyncContext(true, rdbStore);
		}

		if (!context.isFullSyncPossible()) rdbStore.decrementRefCount();
		return context;
	}

	protected String newRdbFileName() {
		return "rdb_" + System.currentTimeMillis() + "_" + UUID.randomUUID().toString();
	}

	@Override
	public FULLSYNC_FAIL_CAUSE fullSyncIfPossible(FullSyncListener fullSyncListener) throws IOException {
		return fullSyncIfPossible(fullSyncListener, false);
	}

	@Override
	public FULLSYNC_FAIL_CAUSE fullSyncIfPossible(FullSyncListener fullSyncListener, boolean tryRordb) throws IOException {
		makeSureOpen();

		if (!fullSyncListener.supportProgress(OffsetReplicationProgress.class)) {
		    return FULLSYNC_FAIL_CAUSE.FULLSYNC_PROGRESS_NOT_SUPPORTED;
		}

		return doFullSyncIfPossible(fullSyncListener, tryRordb);
	}

	protected FULLSYNC_FAIL_CAUSE doFullSyncIfPossible(FullSyncListener fullSyncListener, boolean tryRordb) throws IOException {
		if (fullSyncListener.supportRdb(RdbStore.Type.RORDB)) {
			final FullSyncContext ctx = lockAndCheckIfRordbFullSyncPossible();
			if (ctx.isFullSyncPossible()) return tryDoFullSync(ctx, fullSyncListener);
			else if (tryRordb) return ctx.getCause();
		}

		final FullSyncContext ctx = lockAndCheckIfRdbFullSyncPossible();
		if (ctx.isFullSyncPossible()) {
			return tryDoFullSync(ctx, fullSyncListener);
		} else {
			return ctx.getCause();
		}
	}

	protected FULLSYNC_FAIL_CAUSE tryDoFullSync(FullSyncContext ctx, FullSyncListener fullSyncListener) throws IOException {
		RdbStore rdbStore = ctx.getRdbStore();
		if (null != cmdStore && !cmdStore.retainCommands(
				new DefaultCommandsGuarantee(fullSyncListener, beginOffset(), rdbStore.rdbOffset() + 1, commandsRetainTimeoutMilli))) {
			getLogger().info("[fullSyncToSlave][{}][cmd file deleted and terminate]{}", rdbStore.getRdbType().name(), fullSyncListener);
			rdbStore.decrementRefCount();
			return FULLSYNC_FAIL_CAUSE.MISS_CMD_AFTER_RDB;
		}

		try {
			getLogger().info("[fullSyncToSlave][{}][reuse current rdb to full sync]{}", rdbStore.getRdbType().name(), fullSyncListener);
			// after rdb send over, command will be sent automatically
			rdbStore.readRdbFile(fullSyncListener);
		} finally {
			rdbStore.decrementRefCount();
		}
		return null;
	}

	public void addCommandsListener(ReplicationProgress<?> progress, CommandsListener commandsListener) throws IOException {

		makeSureOpen();

		if (progress instanceof OffsetReplicationProgress) {
			long realOffset = ((OffsetReplicationProgress) progress).getProgress() - metaStore.beginOffset();
			cmdStore.addCommandsListener(new OffsetReplicationProgress(realOffset), commandsListener);
		} else {
			cmdStore.addCommandsListener(progress, commandsListener);
		}
	}

	@Override
	public FULLSYNC_FAIL_CAUSE createIndexIfPossible(ExecutorService indexingExecutors) {
		throw new UnsupportedOperationException();
	}

	@Override
	public GtidSet getBeginGtidSet() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public GtidSet getEndGtidSet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean supportGtidSet() {
		return false;
	}

	@Override
	public boolean isFresh() {
		return metaStore == null || metaStore.isFresh();
	}

	@Override
	public int appendCommands(ByteBuf byteBuf) throws IOException {
		makeSureOpen();

		return cmdStore.appendCommands(byteBuf);
	}

	@Override
	public boolean awaitCommandsOffset(long offset, int timeMilli) throws InterruptedException {
		return cmdStore.awaitCommandsOffset(offset, timeMilli);
	}

	public int getRdbUpdateCount() {
		return rdbUpdateCount.get();
	}

	protected File getBaseDir() {
		return baseDir;
	}

	@Override
	public boolean checkOk() {
		return true;
	}

	@Override
	public String toString() {
		return String.format("ReplicationStore:%s", baseDir);
	}

	public class ReplicationStoreRdbFileListener implements RdbStoreListener {

		protected RdbStore rdbStore;

		public ReplicationStoreRdbFileListener(RdbStore rdbStore) {
			this.rdbStore = rdbStore;
		}

		@Override
		public void onEndRdb() {
			try {
				getLogger().info("[onEndRdb]{}, {}", rdbStore, DefaultReplicationStore.this);
				if(RdbStore.Type.NORMAL.equals(rdbStore.getRdbType())) {
					metaStore.setRdbFileSize(rdbStore.rdbFileLength());
				} else {
					metaStore.setRordbFileSize(rdbStore.rdbFileLength());
				}
			} catch (Exception e) {
				getLogger().error("[onEndRdb]", e);
			}
		}
	}

	@Override
	public void close() throws IOException {

		if (cmpAndSetClosed()) {
			getLogger().info("[close]{}", this);
			RdbStore rdbStore = rdbStoreRef.get();
			if (rdbStore != null) {
				rdbStore.close();
			}

			if (cmdStore != null) {
				cmdStore.close();
			}
		}else{
			getLogger().warn("[close][already closed!]{}", this);
		}
	}

	@Override
	public void destroy() throws Exception {

		getLogger().info("[destroy]{}", this);
		FileUtils.recursiveDelete(baseDir);
	}

	public void releaseRdb() throws IOException {
		synchronized (lock) {
			tryReleaseRdb(rdbStoreRef);
			tryReleaseRdb(rordbStoreRef);
		}
	}

	private void tryReleaseRdb(AtomicReference<RdbStore> rdbStoreRef) throws IOException {
		RdbStore originRdbStore = rdbStoreRef.get();
		if (null != originRdbStore && !originRdbStore.isWriting()
				&& rdbStoreRef.compareAndSet(originRdbStore, null)) {
			getLogger().info("[releaseRdb] {}", originRdbStore);
			previousRdbStores.put(originRdbStore, Boolean.TRUE);
			metaStore.releaseRdbFile(originRdbStore.getRdbFileName());
		}
	}

	private void gcRdbIfNeeded(AtomicReference<RdbStore> rdbStoreRef) throws IOException {
		RdbStore originRdbStore = rdbStoreRef.get();
		if (null != originRdbStore && !originRdbStore.isWriting()
				&& originRdbStore.rdbOffset() + 1 < firstAvailableOffset()
				&& rdbStoreRef.compareAndSet(originRdbStore, null)) {
			getLogger().info("[gc][release rdb for cmd not continue] {}", originRdbStore);
			previousRdbStores.put(originRdbStore, Boolean.TRUE);
			metaStore.releaseRdbFile(originRdbStore.getRdbFileName());
		}
	}

	@Override
	public boolean gc() throws IOException {
		synchronized (lock) {
			gcRdbIfNeeded(rdbStoreRef);
			gcRdbIfNeeded(rordbStoreRef);
		}

		// delete old rdb files
		for (RdbStore rdbStore : previousRdbStores.keySet()) {
			if (rdbStore.refCount() == 0) {
				try {
					rdbStore.destroy();
				} catch (Exception e) {
					getLogger().error("[gc]" + rdbStore, e);
				}
				previousRdbStores.remove(rdbStore);
			}
		}

		// delete old command file
		if (cmdStore != null) {
			cmdStore.gc();
		}
		return true;
	}

	protected Logger getLogger() {
		return logger;
	}

	@VisibleForTesting
	public void setCommandsRetainTimeoutMilli(int timeoutMilli) {
		this.commandsRetainTimeoutMilli = timeoutMilli;
	}

}
