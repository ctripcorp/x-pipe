package com.ctrip.xpipe.redis.keeper.store;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.CommandsListener;
import com.ctrip.xpipe.redis.core.store.DumpedRdbStore;
import com.ctrip.xpipe.redis.core.store.FullSyncListener;
import com.ctrip.xpipe.redis.core.store.MetaStore;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;

import io.netty.buffer.ByteBuf;

// TODO make methods correctly sequenced
public class DefaultReplicationStore implements ReplicationStore {

	private final static Logger logger = LoggerFactory.getLogger(DefaultReplicationStore.class);

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

	private AtomicReference<RdbStore> rdbStoreRef = new AtomicReference<>();

	private ConcurrentMap<RdbStore, Boolean> previousRdbStores = new ConcurrentHashMap<>();

	private DefaultCommandStore cmdStore;

	private MetaStore metaStore;

	private int cmdFileSize;

	private KeeperConfig config;

	private Object lock = new Object();
	
	private AtomicInteger rdbUpdateCount = new AtomicInteger();

	public DefaultReplicationStore(File baseDir, KeeperConfig config, String keeperRunid) throws IOException {
		this.baseDir = baseDir;
		this.cmdFileSize = config.getReplicationStoreCommandFileSize();
		this.config = config;

		metaStore = new DefaultMetaStore(baseDir);
		metaStore.loadMeta();
		metaStore.updateKeeperRunid(keeperRunid);

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
		
		File currentRdbFile = rdbStoreRef.get() == null ? null : rdbStoreRef.get().getRdbFile();

		for (File rdbFile : rdbFilesOnFS()) {
			if (!rdbFile.equals(currentRdbFile)) {
				logger.info("[removeUnusedRdbFile] {}", rdbFile);
				rdbFile.delete();
			}
		}
	}

	@Override
	public void close() throws IOException {
		// TODO
	}

	@Override
	public RdbStore beginRdb(String masterRunid, long masterOffset, long rdbFileSize) throws IOException {
		logger.info("Begin RDB masterRunid:{}, masterOffset:{}, rdbFileSize:{}", masterRunid, masterOffset, rdbFileSize);
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
	public void rdbUpdated(DumpedRdbStore dumpedRdbStore) throws IOException {
		
		synchronized (lock) {
			rdbUpdateCount.incrementAndGet();
			
			File dumpedRdbFile = dumpedRdbStore.getRdbFile();
			if(!baseDir.equals(dumpedRdbFile.getParentFile())){
				throw new IllegalStateException("update rdb error, filePath:" + dumpedRdbFile.getAbsolutePath() + ", baseDir:" + baseDir.getAbsolutePath());
			}
			long rdbFileSize = dumpedRdbStore.getRdbFileSize();
			long masterOffset = dumpedRdbStore.getMasterOffset();

			ReplicationStoreMeta metaDup = metaStore.rdbUpdated(dumpedRdbFile.getName(), rdbFileSize, masterOffset);

			dumpedRdbStore.setRdbLastKeeperOffset(metaDup.getRdbLastKeeperOffset());
			logger.info("[rdbUpdated] new file {}, rdbFileSize {}, masterOffset {}", dumpedRdbFile, rdbFileSize, masterOffset);
			RdbStore oldRdbStore = rdbStoreRef.get();
			rdbStoreRef.set(dumpedRdbStore);
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
			long beginOffset = metaStore.beginOffset();
			long totalLength = cmdStore.totalLength();
			
			logger.debug("[getEndOffset]B:{}, L:{}", beginOffset, totalLength);
			return beginOffset + totalLength - 1;
		}
	}

	@Override
	public MetaStore getMetaStore() {
		return metaStore;
	}

	@Override
	public boolean gc() {
		// delete old rdb files
		for (RdbStore rdbStore : previousRdbStores.keySet()) {
			if (rdbStore.refCount() == 0) {
				File rdbFile = rdbStore.getRdbFile();
				logger.info("[GC] delete rdb file {}", rdbFile);
				rdbFile.delete();
				previousRdbStores.remove(rdbStore);
			}
		}

		// delete old command file
		if (cmdStore != null) {
			for (File cmdFile : cmdFilesOnFS()) {
				long fileStartOffset = cmdStore.extractStartOffset(cmdFile);
				if (canDeleteCmdFile(cmdStore.lowestReadingOffset(), fileStartOffset, cmdFile.length(), cmdFile.lastModified())) {
					logger.info("[GC] delete command file {}", cmdFile);
					cmdFile.delete();
				}
			}
		}

		return true;
	}

	private boolean canDeleteCmdFile(long lowestReadingOffset, long fileStartOffset, long fileSize, long lastModified) {
		return (fileStartOffset + fileSize < lowestReadingOffset)
				&& cmdStore.totalLength() - (fileStartOffset + fileSize) > cmdFileSize * config.getReplicationStoreCommandFileNumToKeep()
				&& (System.currentTimeMillis() - lastModified>= config.getReplicationStoreMinTimeMilliToGcAfterCreate());
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
		File[] files = cmdFilesOnFS();
		
		
		if(files == null || files.length == 0){
			logger.info("[minCmdKeeperOffset][no cmd files][start offset 0]");
			minCmdOffset = 0L;
		}else{
			for (File cmdFile : files) {
				minCmdOffset = Math.min(cmdStore.extractStartOffset(cmdFile), minCmdOffset);
			}
		}

		
		long minCmdKeeperOffset = minCmdOffset + metaStore.getKeeperBeginOffset();
		return minCmdKeeperOffset;
	}

	private long maxCmdKeeperOffset() {
		return metaStore.getKeeperBeginOffset() + (cmdStore == null ? 0 : cmdStore.totalLength()) - 1;
	}

	private FullSyncContext lockAndCheckIfFullSyncPossible() {
		synchronized (lock) {
			RdbStore rdbStore = rdbStoreRef.get();
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
			boolean fullSyncPossible = minCmdKeeperOffset <= rdbLastKeeperOffset + 1 && maxCmdKeeperOffset - rdbLastKeeperOffset <= cmdAfterRdbThreshold;

			logger.info("minCmdKeeperOffset <= rdbLastKeeperOffset + 1 && maxCmdKeeperOffset - rdbLastKeeperOffset <= cmdAfterRdbThreshold");
			logger.info("[isFullSyncPossible] {}, {} <= {} + 1 && {} - {} <= {}", //
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
	public DumpedRdbStore prepareNewRdb() throws IOException {
		
		DumpedRdbStore rdbStore = new DefaultDumpedRdbStore(new File(baseDir, newRdbFileName()));
		return rdbStore;
	}

	@Override
	public boolean fullSyncIfPossible(FullSyncListener fullSyncListener) throws IOException {
		
		final FullSyncContext ctx = lockAndCheckIfFullSyncPossible();
		if (ctx.isFullSyncPossible()) {
			logger.info("[fullSyncToSlave]reuse current rdb to full sync");
			RdbStore rdbStore = ctx.getRdbStore();

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
		
		long realOffset = offset - metaStore.getKeeperBeginOffset();
		getCommandStore().addCommandsListener(realOffset, commandsListener);
	}


	@Override
	public boolean isFresh() {
		return metaStore == null || metaStore.getMasterRunid() == null;
	}

	@Override
	public long getKeeperEndOffset() {
		if (cmdStore == null) {
			throw new RedisKeeperRuntimeException("Command store not initialized, please try later");
		}
		return metaStore.getKeeperBeginOffset() + cmdStore.totalLength() - 1;
	}

	@Override
	public long nextNonOverlappingKeeperBeginOffset() {
		
		long oldKeeperBeginOffset = metaStore.getKeeperBeginOffset();
		long newKeeperBeginOffset = metaStore.getKeeperBeginOffset() + cmdStore.totalLength() + 1;
		logger.info("[nextNonOverlappingKeeperBeginOffset]{}->{}", oldKeeperBeginOffset, newKeeperBeginOffset);
		return newKeeperBeginOffset;
	}

	@Override
	public int appendCommands(ByteBuf byteBuf) throws IOException {
		return cmdStore.appendCommands(byteBuf);
	}

	@Override
	public boolean awaitCommandsOffset(long offset, int timeMilli) throws InterruptedException {
		return cmdStore.awaitCommandsOffset(offset, timeMilli);
	}

	public int getRdbUpdateCount() {
		return rdbUpdateCount.get();
	}
}
