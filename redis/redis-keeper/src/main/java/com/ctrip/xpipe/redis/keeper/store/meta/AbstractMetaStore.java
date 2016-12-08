package com.ctrip.xpipe.redis.keeper.store.meta;

import org.slf4j.LoggerFactory;
import org.unidal.helper.Files.IO;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;

import com.alibaba.fastjson.JSON;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.store.MetaStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import com.ctrip.xpipe.redis.core.store.exception.BadMetaStoreException;
import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperRuntimeException;

/**
 * @author wenchao.meng
 *
 * Dec 4, 2016
 */
public abstract class AbstractMetaStore implements MetaStore{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	

	protected final AtomicReference<ReplicationStoreMeta> metaRef = new AtomicReference<>();

	protected File baseDir;
	
	protected String keeperRunid;

	public AbstractMetaStore(File baseDir, String keeperRunid) {
		this.baseDir = baseDir;
		this.keeperRunid = keeperRunid;
		try {
			loadMeta();
			checkOrSaveKeeperRunid(keeperRunid);
		} catch (IOException e) {
			throw new IllegalStateException("load meta:" + baseDir, e);
		}
	}
	
	private void checkOrSaveKeeperRunid(String keeperRunid) throws IOException {
		synchronized (metaRef) {
			String oldRunId = metaRef.get().getKeeperRunid(); 
			if(oldRunId != null){
				if(!oldRunId.equals(keeperRunid)){
					throw new BadMetaStoreException(oldRunId, keeperRunid); 
				}
			}else{
				ReplicationStoreMeta newMeta = dupReplicationStoreMeta();
				newMeta.setKeeperRunid(keeperRunid);
				saveMeta(newMeta);
			}
		}
		
	}


	protected void saveMetaToFile(File file, ReplicationStoreMeta replicationStoreMeta) throws IOException {
		logger.info("[saveMetaToFile]{}, {}", file, replicationStoreMeta);
		IO.INSTANCE.writeTo(file, JSON.toJSONString(replicationStoreMeta));
	}
	
	protected static ReplicationStoreMeta loadMetaFromFile(File file) throws IOException{
		
		if(file.isFile()){
			return JSON.parseObject(IO.INSTANCE.readFrom(file, "utf-8"), ReplicationStoreMeta.class);
		}
		
		throw new RedisKeeperRuntimeException("[loadMetaFromFile][not file]" + file.getAbsolutePath());
	}

	/**
	 * keeperOffset - keeperBeginOffset == redisOffset - beginOffset
	 */
	@Override
	public long redisOffsetToKeeperOffset(long redisOffset) {
		return redisOffsetToKeeperOffset(redisOffset, metaRef.get());
	}

	private long redisOffsetToKeeperOffset(long redisOffset, ReplicationStoreMeta meta) {
		
		if(meta.getBeginOffset() == null){
			logger.info("[redisOffsetToKeeperOffset][first time create rdb, rdb end set 1]");
			return meta.getKeeperBeginOffset() - 1; 
		}
		return redisOffset - meta.getBeginOffset() + meta.getKeeperBeginOffset();
	}

	@Override
	public void updateKeeperRunid(String keeperRunid) throws IOException {
		
		synchronized (metaRef) {

			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();
			if(metaDup.getKeeperRunid() != null && !metaDup.getKeeperRunid().equals(keeperRunid)){
				logger.warn("[keeperRunIdChanged]{}->{}", metaDup.getKeeperRunid(), keeperRunid);
			}
			metaDup.setKeeperRunid(keeperRunid);;
			saveMeta(metaDup);
		}
		
	}

	@Override
	public long getKeeperBeginOffset() {

		return metaRef.get().getKeeperBeginOffset();
	}

	@Override
	public ReplicationStoreMeta dupReplicationStoreMeta() {
		return new ReplicationStoreMeta(metaRef.get());
	}


	protected void saveMeta(ReplicationStoreMeta newMeta) throws IOException {
		
		logger.info("[Metasaved]\nold:{}\nnew:{}", metaRef.get(), newMeta);
		metaRef.set(newMeta);
		// TODO sync with fs?
		saveMetaToFile(new File(baseDir, META_FILE), metaRef.get());
	}


	@Override
	public void loadMeta() throws IOException {
		
		synchronized (metaRef) {
			
			ReplicationStoreMeta meta = loadMetaCreateIfEmpty(baseDir, META_FILE); 
			metaRef.set(meta);
			logger.info("Meta loaded: {}", meta);
		}
	}
	
	public static ReplicationStoreMeta loadMetaCreateIfEmpty(File baseDir, String fileName) throws IOException{
		
		File metaFile = new File(baseDir, fileName);
		if(metaFile.isFile()){
			ReplicationStoreMeta meta = loadMetaFromFile(metaFile);
			return meta;
		} else {
			return new ReplicationStoreMeta();
		}
		
	}
	
	
	@Override
	public void setKeeperState(String keeperRunid, KeeperState keeperState) throws IOException {
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			metaDup.setKeeperState(keeperState);;
			metaDup.setKeeperRunid(keeperRunid);

			saveMeta(metaDup);
		}
	}

	@Override
	public ReplicationStoreMeta rdbUpdated(String rdbFile, long rdbFileSize, long masterOffset) throws IOException {
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			metaDup.setRdbFile(rdbFile);
			metaDup.setRdbFileSize(rdbFileSize);
			metaDup.setRdbLastKeeperOffset(redisOffsetToKeeperOffset(masterOffset, metaDup));
			logger.info("[rdbUpdated] update RdbLastKeeperOffset to {}", metaDup.getRdbLastKeeperOffset());

			saveMeta(metaDup);

			return metaDup;
		}
	}

	protected ReplicationStoreMeta getMeta() {
		return metaRef.get();
	}

	
	@Override
	public void setMasterAddress(DefaultEndPoint endpoint) throws IOException {
		
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			doSetMasterAddress(metaDup, endpoint);

			saveMeta(metaDup);
		}
	}

	@Override
	public boolean isFresh() {
		return getMeta().getCmdFilePrefix() == null;
	}


	protected abstract void doSetMasterAddress(ReplicationStoreMeta metaDup, DefaultEndPoint endpoint);

	@Override
	public void becomeActive() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void becomeBackup() throws IOException {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public String toString() {
		return String.format("%s, meta:", baseDir, dupReplicationStoreMeta());
	}
}
