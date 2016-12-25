package com.ctrip.xpipe.redis.keeper.store.meta;

import java.io.File;
import java.io.IOException;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;

/**
 * @author wenchao.meng
 *
 * Dec 4, 2016
 */
public class BackupMetaStore extends AbstractMetaStore{
	
	public BackupMetaStore(File baseDir, String keeperRunid) {
		super(baseDir, keeperRunid);
	}

	private static final String ROOT_FILE_PATTERN = "root-%s.json";

	@Override
	public String getMasterRunid() {
		return getMeta().getKeeperRunid();
	}

	@Override
	public Long beginOffset() {
		return getMeta().getKeeperBeginOffset();
	}

	
	@Override
	protected void doSetMasterAddress(ReplicationStoreMeta metaDup, DefaultEndPoint endpoint) {
		metaDup.setActiveKeeperAddress(endpoint);
	}


	@Override
	public DefaultEndPoint getMasterAddress() {
		return getMeta().getActiveKeeperAddress();
	}

	
	@Override
	public void saveKinfo(ReplicationStoreMeta replicationStoreMeta) throws IOException {

		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			metaDup.setMasterRunid(replicationStoreMeta.getMasterRunid());
			metaDup.setMasterAddress(replicationStoreMeta.getMasterAddress());
			
			long delta = getMeta().getKeeperBeginOffset() - replicationStoreMeta.getKeeperBeginOffset();
			metaDup.setBeginOffset(replicationStoreMeta.getBeginOffset() + delta);
			
			logger.info("[saveKinfo]{}, {}, {}", metaDup.getMasterRunid(), metaDup.getMasterAddress(), metaDup.getBeginOffset());
			
			saveMeta(metaDup);
		}
	}

	@Override
	public void psyncBegun(String masterRunid, long keeperBeginOffset) throws IOException {
		logger.info("[psyncBegun][back up nothing to do]{}, {}", masterRunid, keeperBeginOffset);
	}


	@Override
	public ReplicationStoreMeta rdbBegun(String masterRunid, long beginOffset, String rdbFile, EofType eofType,
			String cmdFilePrefix) throws IOException {
		
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			metaDup.setKeeperRunid(masterRunid);
			metaDup.setKeeperBeginOffset(beginOffset);
			metaDup.setRdbFile(rdbFile);
			setRdbFileInfo(metaDup, eofType);
			metaDup.setCmdFilePrefix(cmdFilePrefix);
			metaDup.setRdbLastKeeperOffset(beginOffset - 1);

			saveMeta(metaDup);
			return metaDup;
		}
	}

	@Override
	public void masterChanged(long keeperOffset, DefaultEndPoint newMasterEndpoint, String newMasterRunid,
			long newMasterReplOffset) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public void loadMeta() throws IOException {
		
		synchronized (metaRef) {
			
			super.loadMeta();
			
			//to be compatible with previous data
			try{
				ReplicationStoreMeta currentMeta = dupReplicationStoreMeta();
				ReplicationStoreMeta masterMeta = getReplicationStoreMeta(ReplicationStore.BACKUP_REPLICATION_STORE_REDIS_MASTER_META_NAME);
				if(masterMeta != null && masterMeta.getMasterRunid() != null && !masterMeta.getMasterRunid().equals(currentMeta.getMasterRunid())){
					logger.info("[loadMeta][recover from previous kinfo data]\n{}\n{}", currentMeta, masterMeta);
					
					DefaultEndPoint newMasterAddress = masterMeta.getMasterAddress();
					DefaultEndPoint newKeeperActiveAddress = currentMeta.getMasterAddress();
					String newMasterRunid = masterMeta.getMasterRunid();
					
					long newKeeperBeginOffset = currentMeta.getBeginOffset();
					long newBeginoffset = masterMeta.getBeginOffset() + (newKeeperBeginOffset - masterMeta.getKeeperBeginOffset());
					long newRdbLastOffset = newKeeperBeginOffset + (currentMeta.getRdbLastKeeperOffset() - currentMeta.getKeeperBeginOffset());
					
					ReplicationStoreMeta future = dupReplicationStoreMeta();
					future.setMasterAddress(newMasterAddress);
					future.setActiveKeeperAddress(newKeeperActiveAddress);
					future.setMasterRunid(newMasterRunid);
					future.setKeeperBeginOffset(newKeeperBeginOffset);
					future.setBeginOffset(newBeginoffset);
					future.setRdbLastKeeperOffset(newRdbLastOffset);
					saveMeta(future);
				}
				
			}catch(Exception e){
				logger.info("[loadMeta][load master meta error]" + this + e.getMessage());
			}
		}
		
	}

	private ReplicationStoreMeta getReplicationStoreMeta(String name) throws IOException {
		
		File file = new File(baseDir, getMetaFileName(name));
		ReplicationStoreMeta meta = loadMetaFromFile(file);
		logger.info("[getReplicationStoreMeta]{}, {}, {}", name, meta, file.getAbsolutePath());
		return meta;
	}

	private String getMetaFileName(String name) {
		return String.format(ROOT_FILE_PATTERN, name);
	}
}
