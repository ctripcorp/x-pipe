package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.store.cmd.OffsetCommandReaderWriterFactory;
import com.ctrip.xpipe.redis.keeper.store.cmd.OffsetReplicationProgress;
import com.ctrip.xpipe.redis.keeper.store.meta.DefaultMetaStore;
import com.ctrip.xpipe.utils.FileUtils;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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

	private ConcurrentMap<RdbStore, Boolean> previousRdbStores = new ConcurrentHashMap<>();

	private CommandStore<?,?> cmdStore;

	private MetaStore metaStore;

	private int cmdFileSize;

	private KeeperConfig config;

	private Object lock = new Object();

	private AtomicInteger rdbUpdateCount = new AtomicInteger();

	private KeeperMonitor keeperMonitor;

	public static final String KEY_CMD_RETAIN_TIMEOUT_MILLI = "commandsRetainTimeoutMilli";

	private int commandsRetainTimeoutMilli = Integer.parseInt(System.getProperty(KEY_CMD_RETAIN_TIMEOUT_MILLI, "1800000"));

	private CommandReaderWriterFactory cmdReaderWriterFactory;

	public DefaultReplicationStore(File baseDir, KeeperConfig config, String keeperRunid,
								   KeeperMonitor keeperMonitor) throws IOException {
		this(baseDir, config, keeperRunid, new OffsetCommandReaderWriterFactory(), keeperMonitor);
	}

	public DefaultReplicationStore(File baseDir, KeeperConfig config, String keeperRunid,
								   CommandReaderWriterFactory cmdReaderWriterFactory,
								   KeeperMonitor keeperMonitor) throws IOException {
		this.baseDir = baseDir;
		this.cmdFileSize = config.getReplicationStoreCommandFileSize();
		this.config = config;
		this.keeperMonitor = keeperMonitor;
		this.cmdReaderWriterFactory = cmdReaderWriterFactory;

		metaStore = new DefaultMetaStore(baseDir, keeperRunid);

		ReplicationStoreMeta meta = metaStore.dupReplicationStoreMeta();

		if (meta != null && meta.getRdbFile() != null) {
			File rdb = new File(baseDir, meta.getRdbFile());
			if (rdb.isFile()) {
				rdbStoreRef.set(new DefaultRdbStore(rdb, meta.getRdbLastOffset(), initEofType(meta)));
			}
		}
		if (null != meta && null != meta.getCmdFilePrefix()) {
			cmdStore = createCommandStore(baseDir, meta, cmdFileSize, config, cmdReaderWriterFactory, keeperMonitor);
		}

		removeUnusedRdbFiles();
	}

	private EofType initEofType(ReplicationStoreMeta meta) {

		// must has length field
		getLogger().info("[initEofType][leneof]{}", meta);
		return new LenEofType(meta.getRdbFileSize());
	}

	private void removeUnusedRdbFiles() {

		@SuppressWarnings("resource")
		RdbStore rdbStore = rdbStoreRef.get() == null ? null : rdbStoreRef.get();

		for (File rdbFile : rdbFilesOnFS()) {
			if (rdbStore == null || !rdbStore.sameRdbFile(rdbFile)) {
				getLogger().info("[removeUnusedRdbFile] {}", rdbFile);
				rdbFile.delete();
			}
		}
	}

	@Override
	public RdbStore beginRdb(String replId, long rdbOffset, EofType eofType) throws IOException {

		makeSureOpen();

		getLogger().info("Begin RDB replId:{}, rdbOffset:{}, eof:{}", replId, rdbOffset, eofType);
		baseDir.mkdirs();

		String rdbFile = newRdbFileName();
		String cmdFilePrefix = "cmd_" + UUID.randomUUID().toString() + "_";
		ReplicationStoreMeta newMeta = metaStore.rdbBegun(replId, rdbOffset + 1, rdbFile, eofType,
				cmdFilePrefix);

		// beginOffset - 1 == masteroffset
		RdbStore rdbStore = new DefaultRdbStore(new File(baseDir, newMeta.getRdbFile()),
				newMeta.getBeginOffset() - 1, eofType);
		rdbStore.addListener(new ReplicationStoreRdbFileListener(rdbStore));
		rdbStoreRef.set(rdbStore);
		cmdStore = createCommandStore(baseDir, newMeta, cmdFileSize, config, cmdReaderWriterFactory, keeperMonitor);

		return rdbStoreRef.get();
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
		return new DefaultCommandStore(new File(baseDir, replMeta.getCmdFilePrefix()), cmdFileSize,
				config::getReplicationStoreCommandFileKeepTimeSeconds,
				config.getReplicationStoreMinTimeMilliToGcAfterCreate(),
				config::getReplicationStoreCommandFileNumToKeep,
				config.getCommandReaderFlyingThreshold(),
				cmdReaderWriterFactory, keeperMonitor);
	}

	@Override
	public DumpedRdbStore prepareNewRdb() throws IOException {
		makeSureOpen();

		DumpedRdbStore rdbStore = new DefaultDumpedRdbStore(new File(baseDir, newRdbFileName()));
		return rdbStore;
	}

	@Override
	public void checkReplIdAndUpdateRdb(DumpedRdbStore dumpedRdbStore, String expectedReplId) throws IOException {

		makeSureOpen();

		synchronized (lock) {
			rdbUpdateCount.incrementAndGet();

			File dumpedRdbFile = dumpedRdbStore.getRdbFile();
			if (!baseDir.equals(dumpedRdbFile.getParentFile())) {
				throw new IllegalStateException("update rdb error, filePath:" + dumpedRdbFile.getAbsolutePath()
						+ ", baseDir:" + baseDir.getAbsolutePath());
			}
			EofType eofType = dumpedRdbStore.getEofType();
			long rdbOffset = dumpedRdbStore.rdbOffset();

			@SuppressWarnings("unused")
			ReplicationStoreMeta metaDup = metaStore.checkReplIdAndUpdateRdbInfo(dumpedRdbFile.getName(), eofType, rdbOffset, expectedReplId);

			dumpedRdbStore.addListener(new ReplicationStoreRdbFileListener(dumpedRdbStore));

			getLogger().info("[rdbUpdated] new file {}, eofType {}, rdbOffset {}", dumpedRdbFile, eofType, rdbOffset);
			RdbStore oldRdbStore = rdbStoreRef.get();
			rdbStoreRef.set(dumpedRdbStore);
			if (null!= oldRdbStore) previousRdbStores.put(oldRdbStore, Boolean.TRUE);
		}
	}

	// fot teset only
	public CommandStore getCommandStore() {
		return cmdStore;
	}

	// for test only
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

	private long beginOffset() {
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

		return 0L;
	}

	private File[] rdbFilesOnFS() {
		File[] rdbFiles = baseDir.listFiles(RDB_FILE_FILTER);
		return rdbFiles != null ? rdbFiles : new File[0];
	}

	private FullSyncContext lockAndCheckIfFullSyncPossible() {

		synchronized (lock) {
			RdbStore rdbStore = rdbStoreRef.get();
			if (rdbStore == null || !rdbStore.checkOk()) {
				getLogger().info("[lockAndCheckIfFullSyncPossible][false]{}", rdbStore);
				return new FullSyncContext(false);
			}

			rdbStore.incrementRefCount();
			long rdbOffset = rdbStore.rdbOffset();
			long minOffset = firstAvailableOffset();
			long maxOffset = getEndOffset();

			/**
			 * rdb and cmd is continuous AND not so much cmd after rdb
			 */
			long cmdAfterRdbThreshold = config.getReplicationStoreMaxCommandsToTransferBeforeCreateRdb();
			boolean fullSyncPossible = minOffset <= rdbOffset + 1
					&& (maxOffset - rdbOffset <= cmdAfterRdbThreshold);

			getLogger().info("minOffset <= rdbOffset + 1 && maxOffset - rdbOffset <= cmdAfterRdbThreshold");
			getLogger().info("[isFullSyncPossible] {}, {} <= {} + 1 && {} - {} <= {}", //
					fullSyncPossible, minOffset, rdbOffset, maxOffset, rdbOffset, cmdAfterRdbThreshold);

			if (fullSyncPossible) {
				return new FullSyncContext(true, rdbStore);
			} else {
				rdbStore.decrementRefCount();
				return new FullSyncContext(false);
			}
		}
	}

	private String newRdbFileName() {
		return "rdb_" + System.currentTimeMillis() + "_" + UUID.randomUUID().toString();
	}

	@Override
	public boolean fullSyncIfPossible(FullSyncListener fullSyncListener) throws IOException {
		makeSureOpen();

		final FullSyncContext ctx = lockAndCheckIfFullSyncPossible();
		if (ctx.isFullSyncPossible()) {
			RdbStore rdbStore = ctx.getRdbStore();
			if (null != cmdStore && !cmdStore.retainCommands(
					new DefaultCommandsGuarantee(fullSyncListener, beginOffset(), rdbStore.rdbOffset() + 1, commandsRetainTimeoutMilli))) {
				getLogger().info("[fullSyncToSlave][cmd file deleted and terminate]{}", fullSyncListener);
				rdbStore.decrementRefCount();
				return false;
			}

			try {
				getLogger().info("[fullSyncToSlave][reuse current rdb to full sync]{}", fullSyncListener);
				// after rdb send over, command will be sent automatically
				rdbStore.readRdbFile(fullSyncListener);
			} finally {
				rdbStore.decrementRefCount();
			}
			return true;
		} else {
			return false;
		}
	}

	@Override
	public void addCommandsListener(long offset, CommandsListener commandsListener) throws IOException {
		
		makeSureOpen();

		long realOffset = offset - metaStore.beginOffset();
		getCommandStore().addCommandsListener(new OffsetReplicationProgress(realOffset), commandsListener);
	}

	@Override
	public void addCommandsListener(GtidSet excludedGtidSet, CommandsListener commandsListener) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public GtidSet getBeginGtidSet() throws IOException {
		return getCommandStore().getBeginGtidSet();
	}

	@Override
	public GtidSet getEndGtidSet() {
		return getCommandStore().getEndGtidSet();
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

		private RdbStore rdbStore;

		public ReplicationStoreRdbFileListener(RdbStore rdbStore) {
			this.rdbStore = rdbStore;
		}

		@Override
		public void onEndRdb() {
			try {
				getLogger().info("[onEndRdb]{}, {}", rdbStore, DefaultReplicationStore.this);
				metaStore.setRdbFileSize(rdbStore.rdbFileLength());
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

	@Override
	public boolean gc() throws IOException {
		synchronized (lock) {
			RdbStore originRdbStore = rdbStoreRef.get();
			if (null != originRdbStore && !originRdbStore.isWriting()
					&& originRdbStore.rdbOffset() + 1 < firstAvailableOffset()
					&& rdbStoreRef.compareAndSet(originRdbStore, null)) {
				getLogger().info("[gc][release rdb for cmd not continue] {}", originRdbStore);
				previousRdbStores.put(originRdbStore, Boolean.TRUE);
				metaStore.releaseRdbFile(originRdbStore.getRdbFileName());
			}
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

}
