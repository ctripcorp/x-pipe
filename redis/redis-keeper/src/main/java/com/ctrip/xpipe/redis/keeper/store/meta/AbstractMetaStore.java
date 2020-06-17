package com.ctrip.xpipe.redis.keeper.store.meta;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.MetaStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import com.ctrip.xpipe.redis.core.store.exception.BadMetaStoreException;
import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.helper.Files.IO;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

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
	
	private final void checkOrSaveKeeperRunid(String keeperRunid) throws IOException {
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
		IO.INSTANCE.writeTo(file, Codec.DEFAULT.encode(replicationStoreMeta));
	}
	
	protected static ReplicationStoreMeta loadMetaFromFile(File file) throws IOException{
		
		if(file.isFile()){
			return deserializeFromString(IO.INSTANCE.readFrom(file, "utf-8"));
		}
		
		throw new RedisKeeperRuntimeException("[loadMetaFromFile][not file]" + file.getAbsolutePath());
	}
	
	public static ReplicationStoreMeta deserializeFromString(String str){
		return Codec.DEFAULT.decode(str, ReplicationStoreMeta.class);
	}


	@Override
	public void updateKeeperRunid(String keeperRunid) throws IOException {
		
		synchronized (metaRef) {

			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();
			if(metaDup.getKeeperRunid() != null && !metaDup.getKeeperRunid().equals(keeperRunid)){
				logger.warn("[keeperRunIdChanged]{}->{}", metaDup.getKeeperRunid(), keeperRunid);
			}
			metaDup.setKeeperRunid(keeperRunid);
			saveMeta(metaDup);
		}
	}

	@Override
	public final ReplicationStoreMeta dupReplicationStoreMeta() {
		return new ReplicationStoreMeta(metaRef.get());
	}


	protected final void saveMeta(ReplicationStoreMeta newMeta) throws IOException {
		
		logger.info("[Metasaved]\nold:{}\nnew:{}", metaRef.get(), newMeta);
		metaRef.set(newMeta);
		// TODO sync with fs?
		saveMetaToFile(new File(baseDir, META_FILE), metaRef.get());
	}


	@Override
	public final void loadMeta() throws IOException {
		
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
	public void setRdbFileSize(long rdbFileSize) throws IOException {
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			if(metaDup.getRdbFileSize() != rdbFileSize){
				metaDup.setRdbFileSize(rdbFileSize);
				saveMeta(metaDup);
			}
		}
	}
	
	
	private void setKeeperState(KeeperState keeperState) throws IOException {
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			metaDup.setKeeperState(keeperState);

			saveMeta(metaDup);
		}
	}

	@Override
	public ReplicationStoreMeta rdbUpdated(String rdbFile, EofType eofType, long rdbOffset) throws IOException {
		
		synchronized (metaRef) {
			
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			metaDup.setRdbFile(rdbFile);
			setRdbFileInfo(metaDup, eofType);
			metaDup.setRdbLastOffset(rdbOffset);
			
			logger.info("[rdbUpdated] update rdbLastOffset to {}", rdbOffset);
			saveMeta(metaDup);

			return metaDup;
		}
	}

	protected void setRdbFileInfo(ReplicationStoreMeta metaDup, EofType eofType) {
		
		String tag = eofType.getTag(); 
		
		if(tag.length() == RedisClientProtocol.RUN_ID_LENGTH){
			metaDup.setRdbEofMark(tag);
			metaDup.setRdbFileSize(-1);
		}else{
			metaDup.setRdbFileSize(Long.valueOf(tag));
			metaDup.setRdbEofMark(null);
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
		setKeeperState(KeeperState.ACTIVE);
	}

	@Override
	public void becomeBackup() throws IOException {
		setKeeperState(KeeperState.BACKUP);
	}
	
	@Override
	public String toString() {
		return String.format("%s, meta:", baseDir, dupReplicationStoreMeta());
	}
}
