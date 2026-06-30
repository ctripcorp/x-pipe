package com.ctrip.xpipe.redis.keeper.store.meta;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.core.store.exception.BadMetaStoreException;
import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperRuntimeException;
import com.ctrip.xpipe.redis.keeper.exception.replication.UnexpectedReplIdException;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
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

	protected final AsyncFileSystem asyncFileSystem;

	public AbstractMetaStore(File baseDir, String keeperRunid, AsyncFileSystem asyncFileSystem) {
		this.baseDir = baseDir;
		this.keeperRunid = keeperRunid;
		this.asyncFileSystem = Objects.requireNonNull(asyncFileSystem, "asyncFileSystem");
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


	protected void saveMetaToFileV2(File file, ReplicationStoreMeta replicationStoreMeta) throws IOException {
		logger.info("[saveMetaToFileV2]{}, {}", file, replicationStoreMeta);
		byte[] data = Codec.DEFAULT.encode(replicationStoreMeta).getBytes(StandardCharsets.UTF_8);
		AsyncFile asyncFile = AsyncFileSystemHelper.await(asyncFileSystem.open(file.getAbsolutePath(), true, true, true),
				"open meta for write " + file.getAbsolutePath());
		try {
			AsyncFileSystemHelper.await(asyncFileSystem.write(asyncFile, data, data.length),
					"write meta " + file.getAbsolutePath());
		} finally {
			AsyncFileSystemHelper.await(asyncFileSystem.close(asyncFile), "close meta " + file.getAbsolutePath());
		}
	}

	protected ReplicationStoreMeta loadMetaFromFileV2(File file) throws IOException{

		if(AsyncFileSystemHelper.await(asyncFileSystem.exists(file.getAbsolutePath()),
				"check meta exists " + file.getAbsolutePath())){
			AsyncFile asyncFile = AsyncFileSystemHelper.await(asyncFileSystem.open(file.getAbsolutePath(), false, false, true),
					"open meta for read " + file.getAbsolutePath());
			try {
				long size = AsyncFileSystemHelper.await(asyncFileSystem.size(asyncFile),
						"stat meta " + file.getAbsolutePath());
				if (size > Integer.MAX_VALUE) {
					throw new IOException("async file too large: " + file.getAbsolutePath());
				}
				byte[] data = new byte[(int) size];
				int read = AsyncFileSystemHelper.await(asyncFileSystem.read(asyncFile, size, 0, data),
						"read meta " + file.getAbsolutePath());
				if (read != size) {
					throw new IOException("failed to read full async file: " + file.getAbsolutePath());
				}
				return deserializeFromStringV2(new String(data, StandardCharsets.UTF_8));
			} finally {
				AsyncFileSystemHelper.await(asyncFileSystem.close(asyncFile), "close meta " + file.getAbsolutePath());
			}
		}

		throw new RedisKeeperRuntimeException("[loadMetaFromFileV2][not file]" + file.getAbsolutePath());
	}

	public static ReplicationStoreMeta deserializeFromStringV2(String str){
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
	public ReplicationStoreMeta dupReplicationStoreMeta() {
		return new ReplicationStoreMeta(metaRef.get());
	}


	protected final void saveMeta(ReplicationStoreMeta newMeta) throws IOException {
		synchronized (metaRef) {
			logger.info("[Metasaved]\nold:{}\nnew:{}", metaRef.get(), newMeta);
			metaRef.set(newMeta);
			saveMetaToFileV2(new File(baseDir, META_V2_FILE), metaRef.get());
		}
	}


	@Override
	public final void loadMeta() throws IOException {
		
		synchronized (metaRef) {
			File metaV2File = new File(baseDir, META_V2_FILE);
			ReplicationStoreMeta meta;
			File source;
			if (AsyncFileSystemHelper.await(asyncFileSystem.exists(metaV2File.getAbsolutePath()),
					"check meta exists " + metaV2File.getAbsolutePath())) {
				meta = loadMetaFromFileV2(metaV2File);
				source = metaV2File;
			} else {
				meta = new ReplicationStoreMeta();
				source = null;
			}
			metaRef.set(meta);
			logger.info("Meta loaded: {}, source:{}", meta, source);
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

	@Override
	public void setRordbFileSize(long rordbFileSize) throws IOException {
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			if(metaDup.getRordbFileSize() != rordbFileSize){
				metaDup.setRordbFileSize(rordbFileSize);
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
	public ReplicationStoreMeta checkReplIdAndUpdateRdbInfo(String rdbFile, RdbStore.Type type, EofType eofType,
															long rdbOffset, String gtidSet, String expectedReplId) throws IOException {
		synchronized (metaRef) {

			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			if (!Objects.equals(expectedReplId, metaDup.getReplId())) {
				throw new UnexpectedReplIdException(expectedReplId, metaDup.getReplId());
			}

			if (RdbStore.Type.NORMAL.equals(type)) {
				logger.info("[rdbUpdated] update rdbLastOffset to {}", rdbOffset);
				metaDup.setRdbFile(rdbFile);
				setRdbFileInfo(metaDup, eofType);
				metaDup.setRdbLastOffset(rdbOffset);
				metaDup.setRdbGtidSet(gtidSet);
			} else if (RdbStore.Type.RORDB.equals(type)) {
				logger.info("[rordbUpdated] update rordbLastOffset to {}", rdbOffset);
				metaDup.setRordbFile(rdbFile);
				setRordbFileInfo(metaDup, eofType);
				metaDup.setRordbLastOffset(rdbOffset);
				metaDup.setRordbGtidSet(gtidSet);
			} else {
				throw new IllegalStateException("unknown type " + (type == null?"null":type.name()));
			}

			saveMeta(metaDup);

			return metaDup;
		}
	}

	@Override
	public ReplicationStoreMeta checkReplIdAndUpdateRdbInfo(String rdbFile, EofType eofType, long rdbOffset, String expectedReplId) throws IOException {
		
		synchronized (metaRef) {
			
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			if (!Objects.equals(expectedReplId, metaDup.getReplId())) {
			    throw new UnexpectedReplIdException(expectedReplId, metaDup.getReplId());
			}

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

	protected void setRordbFileInfo(ReplicationStoreMeta metaDup, EofType eofType) {

		String tag = eofType.getTag();

		if(tag.length() == RedisClientProtocol.RUN_ID_LENGTH){
			metaDup.setRordbEofMark(tag);
			metaDup.setRordbFileSize(-1);
		}else{
			metaDup.setRordbFileSize(Long.valueOf(tag));
			metaDup.setRordbEofMark(null);
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
