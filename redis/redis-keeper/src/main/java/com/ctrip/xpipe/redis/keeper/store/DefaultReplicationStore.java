package com.ctrip.xpipe.redis.keeper.store;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.CommandsListener;
import com.ctrip.xpipe.redis.core.store.FullSyncListener;
import com.ctrip.xpipe.redis.core.store.MetaStore;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;

import io.netty.buffer.ByteBuf;

// TODO make methods correctly sequenced
public class DefaultReplicationStore implements ReplicationStore {

	private final static Logger log = LoggerFactory.getLogger(DefaultReplicationStore.class);

	private final static FileFilter RDB_FILE_FILTER = new FileFilter() {

		@Override
		public boolean accept(File path) {
			return path.isFile() && path.getName().startsWith("rdb_");
		}
	};

	private final static FileFilter CMD_FILE_FILTER = new FileFilter() {

		@Override
		public boolean accept(File path) {
			return path.isFile() && path.getName().startsWith("cmd_");
		}
	};

	private File baseDir;

	private AtomicReference<DefaultRdbStore> rdbStoreRef = new AtomicReference<>();

	private ConcurrentMap<DefaultRdbStore, Boolean> previousRdbStores = new ConcurrentHashMap<>();

	private DefaultCommandStore cmdStore;

	private MetaStore metaStore;

	private int cmdFileSize;

	private KeeperConfig config;

	private Object lock = new Object();

	public DefaultReplicationStore(File baseDir, KeeperConfig config) throws IOException {
		this.baseDir = baseDir;
		this.cmdFileSize = config.getReplicationStoreCommandFileSize();
		this.config = config;

		metaStore = new DefaultMetaStore(baseDir);
		metaStore.loadMeta();

		ReplicationStoreMeta meta = metaStore.dupReplicationStoreMeta();

		if (meta.getRdbFile() != null) {
			File rdb = new File(baseDir, meta.getRdbFile());
			if (rdb.isFile()) {
				rdbStoreRef.set(new DefaultRdbStore(rdb, meta.getKeeperBeginOffset() - 1, meta.getRdbFileSize()));
				cmdStore = new DefaultCommandStore(new File(baseDir, meta.getCmdFilePrefix()), cmdFileSize);
			}
		}

		removeUnusedRdbFiles();
	}

	private void removeUnusedRdbFiles() {
		File currentRdbFile = rdbStoreRef.get() == null ? null : rdbStoreRef.get().getFile();

		for (File rdbFile : rdbFilesOnFS()) {
			if (!rdbFile.equals(currentRdbFile)) {
				log.info("[removeUnusedRdbFile] {}", rdbFile);
			}
		}
	}

	@Override
	public void close() throws IOException {
		// TODO
	}

	@Override
	public RdbStore beginRdb(String masterRunid, long masterOffset, long rdbFileSize) throws IOException {
		log.info("Begin RDB masterRunid:{}, masterOffset:{}, rdbFileSize:{}", masterRunid, masterOffset, rdbFileSize);
		baseDir.mkdirs();

		String rdbFile = newRdbFileName();
		String cmdFilePrefix = "cmd_" + UUID.randomUUID().toString() + "_";
		ReplicationStoreMeta newMeta = metaStore.rdbBegun(masterRunid, masterOffset + 1, rdbFile, rdbFileSize, cmdFilePrefix);

		// beginOffset - 1 == masteroffset
		rdbStoreRef.set(new DefaultRdbStore(new File(baseDir, newMeta.getRdbFile()), newMeta.getKeeperBeginOffset() - 1, rdbFileSize));
		cmdStore = new DefaultCommandStore(new File(baseDir, newMeta.getCmdFilePrefix()), cmdFileSize);

		return rdbStoreRef.get();
	}

	@Override
	public void delete() {
		// TODO Auto-generated method stub

	}

	@Override
	public void rdbUpdated(String rdbRelativePath, long masterOffset) throws IOException {
		synchronized (lock) {
			File rdbFile = new File(baseDir, rdbRelativePath);
			long rdbFileSize = rdbFile.length();

			ReplicationStoreMeta metaDup = metaStore.rdbUpdated(rdbRelativePath, rdbFileSize, masterOffset);

			log.info("[rdbUpdated] new file {}, rdbFileSize {}, masterOffset {}", rdbFile, rdbFileSize, masterOffset);
			DefaultRdbStore oldRdbStore = rdbStoreRef.get();
			rdbStoreRef.set(new DefaultRdbStore(rdbFile, metaDup.getRdbLastKeeperOffset(), rdbFileSize, true));
			previousRdbStores.put(oldRdbStore, Boolean.TRUE);
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
	public long getEndOffset() {
		if (metaStore == null || metaStore.beginOffset() == null) {
			// TODO
			return -2L;
		} else {
			return metaStore.beginOffset() + cmdStore.totalLength() - 1;
		}
	}

	@Override
	public MetaStore getMetaStore() {
		return metaStore;
	}

	@Override
	public boolean gc() {
		// delete old rdb files
		for (DefaultRdbStore rdbStore : previousRdbStores.keySet()) {
			if (rdbStore.refCount() == 0) {
				File rdbFile = rdbStore.getFile();
				log.info("[GC] delete rdb file {}", rdbFile);
				rdbFile.delete();
				previousRdbStores.remove(rdbStore);
			}
		}

		// delete old command file
		if (cmdStore != null) {
			for (File cmdFile : cmdFilesOnFS()) {
				long fileStartOffset = cmdStore.extractStartOffset(cmdFile);
				if (canDeleteCmdFile(cmdStore.lowestReadingOffset(), fileStartOffset, cmdFile.length())) {
					log.info("[GC] delete command file {}", cmdFile);
					cmdFile.delete();
				}
			}
		}

		return true;
	}

	private boolean canDeleteCmdFile(long lowestReadingOffset, long fileStartOffset, long fileSize) {
		return (fileStartOffset + fileSize < lowestReadingOffset)
				&& cmdStore.totalLength() - (fileStartOffset + fileSize) > cmdFileSize * config.getReplicationStoreCommandFileNumToKeep();
	}

	private File[] rdbFilesOnFS() {
		File[] rdbFiles = baseDir.listFiles(RDB_FILE_FILTER);
		return rdbFiles != null ? rdbFiles : new File[0];
	}

	private File[] cmdFilesOnFS() {
		File[] cmdFiles = baseDir.listFiles(CMD_FILE_FILTER);
		return cmdFiles != null ? cmdFiles : new File[0];
	}

	private long minCmdKeeperOffset() {
		long minCmdOffset = Long.MAX_VALUE; // start from zero

		for (File cmdFile : cmdFilesOnFS()) {
			minCmdOffset = Math.min(cmdStore.extractStartOffset(cmdFile), minCmdOffset);
		}

		long minCmdKeeperOffset = minCmdOffset + metaStore.getKeeperBeginOffset();

		return minCmdKeeperOffset;
	}

	private long maxCmdKeeperOffset() {
		return metaStore.getKeeperBeginOffset() + (cmdStore == null ? 0 : cmdStore.totalLength());
	}

	private FullSyncContext lockAndCheckIfFullSyncPossible() {
		synchronized (lock) {
			DefaultRdbStore rdbStore = rdbStoreRef.get();
			if (rdbStore == null) {
				return new FullSyncContext(false);
			}

			rdbStore.incrementRefCount();
			long rdbLastKeeperOffset = rdbStore.lastKeeperOffset();
			long minCmdKeeperOffset = minCmdKeeperOffset();
			long maxCmdKeeperOffset = maxCmdKeeperOffset();

			/**
			 * rdb and cmd is continuous AND not so much cmd after rdb
			 */
			long cmdAfterRdbThreshold = config.getReplicationStoreMaxCommandsToTransferBeforeCreateRdb();
			boolean fullSyncPossible = minCmdKeeperOffset <= rdbLastKeeperOffset + 1 && maxCmdKeeperOffset - rdbLastKeeperOffset < cmdAfterRdbThreshold;

			log.info("[isFullSyncPossible] {}, {} <= {} + 1 && {} - {} < {}", //
					fullSyncPossible, minCmdKeeperOffset, rdbLastKeeperOffset, maxCmdKeeperOffset, rdbLastKeeperOffset, cmdAfterRdbThreshold);

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
	public File prepareNewRdbFile() {
		return new File(baseDir, newRdbFileName());
	}

	@Override
	public boolean fullSyncIfPossible(FullSyncListener fullSyncListener) throws IOException {
		final FullSyncContext ctx = lockAndCheckIfFullSyncPossible();
		if (ctx.isFullSyncPossible()) {
			log.info("[fullSyncToSlave]reuse current rdb to full sync");
			DefaultRdbStore rdbStore = ctx.getRdbStore();

			try {
				rdbStore.readRdbFile(fullSyncListener);
			} finally {
				rdbStore.decrementRefCount();
			}

			if (fullSyncListener.isOpen()) {
				addCommandsListener(rdbStore.lastKeeperOffset() + 1, fullSyncListener);
			}
			return true;
		} else {
			return false;
		}
	}

	@Override
	public void addCommandsListener(long offset, CommandsListener commandsListener) throws IOException {
		
		long commandOffset = offset - getMetaStore().getKeeperBeginOffset();
		getCommandStore().addCommandsListener(commandOffset, commandsListener);
	}


	@Override
	public boolean isFresh() {
		return metaStore == null || metaStore.getMasterRunid() == null;
	}

	@Override
	public long getKeeperEndOffset() {
		return metaStore.getKeeperBeginOffset() + cmdStore.totalLength() - 1;
	}

	@Override
	public long nextNonOverlappingKeeperBeginOffset() {
		return metaStore.getKeeperBeginOffset() + cmdStore.totalLength() + 1;
	}

	@Override
	public int appendCommands(ByteBuf byteBuf) throws IOException {
		return cmdStore.appendCommands(byteBuf);
	}

	@Override
	public boolean awaitCommandsOffset(long offset, int timeMilli) throws InterruptedException {
		return cmdStore.awaitCommandsOffset(offset, timeMilli);
	}

}
