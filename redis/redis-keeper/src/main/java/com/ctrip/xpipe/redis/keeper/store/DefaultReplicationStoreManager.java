package com.ctrip.xpipe.redis.keeper.store;


import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author marsqing
 *
 *         May 31, 2016 5:33:46 PM
 */
public class DefaultReplicationStoreManager implements ReplicationStoreManager {

	private final static String META_FILE = "store_manager_meta.properties";

	private static final String LATEST_STORE_DIR = "latest.store.dir";

	private Logger logger = LoggerFactory.getLogger(getClass());

	private String clusterName;

	private String shardName;
	
	private String keeperRunid;

	private File baseDir;

	private File metaFile;

	private AtomicReference<Properties> currentMeta = new AtomicReference<Properties>();

	private AtomicReference<DefaultReplicationStore> currentStore = new AtomicReference<>();

	private KeeperConfig keeperConfig;

	public DefaultReplicationStoreManager(KeeperConfig keeperConfig, String clusterName, String shardName, String keeperRunid, File baseDir) {
		this.clusterName = clusterName;
		this.shardName = shardName;
		this.keeperRunid = keeperRunid;
		this.keeperConfig = keeperConfig;
		this.baseDir = new File(baseDir, clusterName + "/" + shardName);
		metaFile = new File(this.baseDir, META_FILE);

		// TODO move to manager of manager and add "close" logic
		Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("gc", true)).scheduleWithFixedDelay(new Runnable() {

			@Override
			public void run() {
				try {
					gc();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}, keeperConfig.getReplicationStoreGcIntervalSeconds(), keeperConfig.getReplicationStoreGcIntervalSeconds(), TimeUnit.SECONDS);
	}

	@Override
	public synchronized ReplicationStore createIfNotExist() throws IOException {

		ReplicationStore currentReplicationStore = null;
		
		try{
			currentReplicationStore = getCurrent();
		}catch(Exception e){
			logger.error("[createIfNotExist]" + baseDir, e);
		}
		
		if (currentReplicationStore == null) {
			logger.info("[createIfNotExist]{}", baseDir);
			currentReplicationStore = create();
		}
		return currentReplicationStore;
	}
	
	@Override
	public synchronized ReplicationStore create(String masterRunid, long keeperBeginOffset) throws IOException {
		ReplicationStore replicationStore = create();
		
		replicationStore.getMetaStore().psyncBegun(masterRunid, keeperBeginOffset);
		
		return replicationStore;
	}

	@Override
	public synchronized DefaultReplicationStore create() throws IOException {
		// TODO dir naming

		File storeBaseDir = new File(baseDir, UUID.randomUUID().toString());
		storeBaseDir.mkdirs();

		logger.info("[create]{}", storeBaseDir);

		recrodLatestStore(storeBaseDir.getName());

		currentStore.set(new DefaultReplicationStore(storeBaseDir, keeperConfig, keeperRunid));
		return currentStore.get();
	}

	/**
	 * @param name
	 * @throws IOException
	 */
	private void recrodLatestStore(String storeDir) throws IOException {
		Properties meta = currentMeta();
		if (meta == null) {
			meta = new Properties();
		}

		meta.setProperty(LATEST_STORE_DIR, storeDir);

		saveMeta(meta);
	}

	/**
	 * @param meta
	 * @throws IOException
	 */
	private void saveMeta(Properties meta) throws IOException {
		try (Writer writer = new FileWriter(metaFile)) {
			meta.store(writer, null);
		}
		currentMeta.set(meta);
	}

	/**
	 * @return
	 * @throws IOException
	 */
	private Properties loadMeta() throws IOException {
		if (metaFile.isFile()) {
			Properties meta = new Properties();
			try (Reader reader = new FileReader(metaFile)) {
				meta.load(reader);
			}
			return meta;
		}

		return null;
	}

	private Properties currentMeta() throws IOException {
		if (currentMeta.get() == null) {
			currentMeta.set(loadMeta());
		}
		return currentMeta.get();
	}

	@Override
	public synchronized ReplicationStore getCurrent() throws IOException {
		// TODO read-write lock
		if (currentStore.get() == null) {
			Properties meta = currentMeta();
			if (meta != null) {
				if (meta.getProperty(LATEST_STORE_DIR) != null) {
					File latestStoreDir = new File(baseDir, meta.getProperty(LATEST_STORE_DIR));
					logger.info("[getCurrent][latest]{}", latestStoreDir);
					if (latestStoreDir.isDirectory()) {
						currentStore.set(new DefaultReplicationStore(latestStoreDir, keeperConfig, keeperRunid));
					}
				}
			}
		}

		return currentStore.get();
	}

	@Override
	public void destroy(ReplicationStore replicationStore) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getClusterName() {
		return clusterName;
	}

	@Override
	public String getShardName() {
		return shardName;
	}

	private void gc() throws IOException {
		// remove directories of previous ReplicationStores
		Properties meta = currentMeta.get();
		if (meta != null) {
			final String currentDirName = meta.getProperty(LATEST_STORE_DIR);
			File[] ReplicationStoreDirs = baseDir.listFiles(new FileFilter() {

				@Override
				public boolean accept(File path) {
					return path.isDirectory() && !currentDirName.equals(path.getName());
				}
			});

			if (ReplicationStoreDirs != null) {
				for (File dir : ReplicationStoreDirs) {
					logger.info("[GC] directory {}", dir.getCanonicalPath());
					FileUtils.deleteDirectory(dir);
				}
			}
		}

		// gc current ReplicationStore
		ReplicationStore replicationStore = getCurrent();
		if (replicationStore != null) {
			replicationStore.gc();
		}
	}

	@Override
	public void destroy() {
		logger.info("[destroy]{}", this.baseDir);
		recursiveDelete(this.baseDir);
	}

	//helper method to clean created files
	private void recursiveDelete(File file) {
		if (!file.exists() || !file.canWrite()) {
			return;
		}
		if (file.isDirectory()) {
			File[] children = file.listFiles();
			if (children != null && children.length > 0) {
				for (File f : children) {
					recursiveDelete(f);
				}
			}
		}
		logger.info("[recursiveDelete]{}", file.getAbsolutePath());
		file.delete();
	}
}
