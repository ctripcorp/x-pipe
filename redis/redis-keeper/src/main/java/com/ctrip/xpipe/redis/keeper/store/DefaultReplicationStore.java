package com.ctrip.xpipe.redis.keeper.store;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.MetaStore;
import com.ctrip.xpipe.redis.core.store.RdbFileListener;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;

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

	private AtomicReference<DefaultRdbStore> rdbStore = new AtomicReference<>();

	private ConcurrentMap<DefaultRdbStore, Boolean> previousRdbStores = new ConcurrentHashMap<>();

	private DefaultCommandStore cmdStore;

	private MetaStore metaStore;

	private int cmdFileSize;

	public DefaultReplicationStore(File baseDir, int cmdFileSize) throws IOException {
		this.baseDir = baseDir;
		this.cmdFileSize = cmdFileSize;

		metaStore = new DefaultMetaStore(baseDir);
		metaStore.loadMeta();

		ReplicationStoreMeta meta = metaStore.dupReplicationStoreMeta();

		if (meta.getRdbFile() != null) {
			File rdb = new File(baseDir, meta.getRdbFile());
			if (rdb.isFile()) {
				rdbStore.set(new DefaultRdbStore(rdb, meta.getKeeperBeginOffset() - 1, meta.getRdbFileSize()));
				cmdStore = new DefaultCommandStore(new File(baseDir, meta.getCmdFilePrefix()), cmdFileSize);
			}
		}
	}
	
	@Override
	public void close() throws IOException {
		// TODO
	}

	@Override
	public void beginRdb(String masterRunid, long masterOffset, long rdbFileSize) throws IOException {
		log.info("Begin RDB masterRunid:{}, masterOffset:{}, rdbFileSize:{}", masterRunid, masterOffset, rdbFileSize);
		baseDir.mkdirs();

		String rdbFile = newRdbFileName();
		String cmdFilePrefix = "cmd_" + UUID.randomUUID().toString() + "_";
		ReplicationStoreMeta newMeta = metaStore.rdbBegun(masterRunid, masterOffset + 1, rdbFile, rdbFileSize, cmdFilePrefix);

		// TODO file naming
		// beginOffset - 1 == masteroffset
		rdbStore.set(new DefaultRdbStore(new File(baseDir, newMeta.getRdbFile()), newMeta.getKeeperBeginOffset() - 1, rdbFileSize));
		cmdStore = new DefaultCommandStore(new File(baseDir, newMeta.getCmdFilePrefix()), cmdFileSize);
	}

	@Override
	public void delete() {
		// TODO Auto-generated method stub

	}

	@Override
	public synchronized void rdbUpdated(String rdbRelativePath, long masterOffset) throws IOException {
		File rdbFile = new File(baseDir, rdbRelativePath);
		long rdbFileSize = rdbFile.length();

		ReplicationStoreMeta metaDup = metaStore.rdbUpdated(rdbRelativePath, rdbFileSize, masterOffset);

		log.info("[rdbUpdated] new file {}, rdbFileSize {}", rdbFile, rdbFileSize);
		previousRdbStores.put(rdbStore.get(), Boolean.TRUE);
		rdbStore.set(new DefaultRdbStore(rdbFile, metaDup.getRdbLastKeeperOffset(), rdbFileSize, true));
	}

	@Override
	public CommandStore getCommandStore() {
		return cmdStore;
	}

	@Override
	public RdbStore getRdbStore() {
		// TODO
		while (rdbStore.get() == null) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
			}
		}
		return rdbStore.get();
	}

	@Override
	public long endOffset() {
		if (cmdStore == null) {
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
		if (rdbStore.get() != null) {
			Set<File> rdbFilesCurrentlyUsing = new HashSet<>();

			File currentRdbFile = rdbStore.get().getFile();
			rdbFilesCurrentlyUsing.add(currentRdbFile);

			for (DefaultRdbStore rdbStore : previousRdbStores.keySet()) {
				if (rdbStore.refCount() > 0) {
					rdbFilesCurrentlyUsing.add(rdbStore.getFile());
				}
			}

			for (File rdbFile : rdbFilesOnFS()) {
				if (!rdbFilesCurrentlyUsing.contains(rdbFile)) {
					log.info("[GC] delete rdb file {}", rdbFile);
					rdbFile.delete();
				}
			}
		}

		// delete old command file
		if (cmdStore != null) {
			for (File cmdFile : cmdFilesOnFS()) {
				long fileStartOffset = cmdStore.extractStartOffset(cmdFile);
				if (canDeleteCmdFile(cmdStore.lowestReadingOffset(), fileStartOffset, cmdFile.length())) {
					// TODO
					log.info("[GC] delete command file {}", cmdFile);
					cmdFile.delete();
				}
			}
		}

		return true;
	}

	private boolean canDeleteCmdFile(long lowestReadingOffset, long fileStartOffset, long fileSize) {
		// TODO
		return fileStartOffset < lowestReadingOffset && cmdStore.totalLength() - (fileStartOffset + fileSize) > cmdFileSize * 2;
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
		return metaStore.getKeeperBeginOffset() + cmdStore.totalLength();
	}

	private boolean isFullSyncPossible() {
		if(rdbStore.get() == null) {
			return false;
		}
		
		// TODO temporary stop remove command file
		long rdbLastKeeperOffset = rdbStore.get().lastKeeperOffset();
		long minCmdKeeperOffset = minCmdKeeperOffset();
		long maxCmdKeeperOffset = maxCmdKeeperOffset();

		/**
		 * rdb and cmd is continuous AND not so much cmd after rdb
		 */
		// TODO
		long cmdAfterRdbThreshold = 100;
		boolean fullSyncPossible = minCmdKeeperOffset <= rdbLastKeeperOffset + 1 && maxCmdKeeperOffset - rdbLastKeeperOffset < cmdAfterRdbThreshold;
		
		log.info("[isFullSyncPossible] {}, {} <= {} + 1 && {} - {} < {}", //
				fullSyncPossible, minCmdKeeperOffset, rdbLastKeeperOffset, maxCmdKeeperOffset, rdbLastKeeperOffset, cmdAfterRdbThreshold);

		return fullSyncPossible;
	}

	private String newRdbFileName() {
		return "rdb_" + System.currentTimeMillis() + "_" + UUID.randomUUID().toString();
	}

	@Override
	public File newRdbFile() {
		return new File(baseDir, newRdbFileName());
	}

	@Override
	public boolean fullSyncIfPossible(RdbFileListener rdbFileListener) throws IOException {
		if (isFullSyncPossible()) {
			log.info("[fullSyncToSlave]reuse current rdb to full sync");
			rdbStore.get().readRdbFile(rdbFileListener);
			return true;
		} else {
			return false;
		}
	}

}
