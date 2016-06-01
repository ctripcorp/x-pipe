package com.ctrip.xpipe.redis.protocal.cmd;


import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.keeper.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.ReplicationStoreManager;
import com.ctrip.xpipe.redis.protocal.CmdContext;
import com.ctrip.xpipe.redis.protocal.PsyncObserver;
import com.ctrip.xpipe.redis.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.protocal.protocal.BulkStringParser;
import com.ctrip.xpipe.redis.protocal.protocal.BulkStringParser.BulkStringParserListener;
import com.ctrip.xpipe.redis.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.utils.StringUtil;

import io.netty.buffer.ByteBuf;


/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午2:24:38
 */
public class Psync extends AbstractRedisCommand implements BulkStringParserListener{
	
	
	public static final String FULL_SYNC = "FULLRESYNC";
	public static final String PARTIAL_SYNC = "CONTINUE";
		
	private BulkStringParser rdbReader;
	
	private ReplicationStoreManager replicationStoreManager;
	private volatile ReplicationStore  	    currentReplicationStore;
	
	private String masterRunid;
	private long   offset;

	private List<PsyncObserver> observers = new LinkedList<PsyncObserver>();
	
	private PSYNC_STATE psyncState = PSYNC_STATE.PSYNC_COMMAND_WAITING_REPONSE; 
	
	public static enum PSYNC_STATE{
		PSYNC_COMMAND_WAITING_REPONSE,
		READING_RDB,
		READING_COMMANDS
	}
	
	
	public Psync(ReplicationStoreManager replicationStoreManager) {
		
		this.replicationStoreManager = replicationStoreManager;
		currentReplicationStore = getCurrentReplicationStore();
	}
	
	
	@Override
	public String getName() {
		return "psync";
	}
	
	
	public void addPsyncObserver(PsyncObserver observer){
		this.observers.add(observer);
	}


	@Override
	protected ByteBuf doRequest() {
		
		String masterRunidRequest = null;
		
		if(currentReplicationStore == null){
			masterRunidRequest = "?";
			offset = -1;
		}else{
			masterRunidRequest = currentReplicationStore.getMasterRunid();
			offset = currentReplicationStore.endOffset() + 1;
		}
		if(masterRunidRequest == null){
			masterRunidRequest = "?";
			offset = -1;
		}
		RequestStringParser requestString = new RequestStringParser(getName(), masterRunidRequest, String.valueOf(offset));
		logger.info("[doRequest]", StringUtil.join(" ", requestString.getPayload()));
		return requestString.format();
	}

	private ReplicationStore getCurrentReplicationStore() {
		
		try {
			return replicationStoreManager.getCurrent();
		} catch (IOException e) {
			logger.error("[doRequest]" + replicationStoreManager, e);
			throw new XpipeRuntimeException("[doRequest]getReplicationStore failed." + replicationStoreManager, e);
		}
	}


	@Override
	public String toString() {
		
		return getName() + "," + getCurrentReplicationStore();
	}
	
	@Override
	protected void doConnectionClosed() {
		super.doConnectionClosed();
		
		switch(psyncState){
			case PSYNC_COMMAND_WAITING_REPONSE:
				break;
			case READING_RDB:
				endReadRdb();
				break;
			case READING_COMMANDS:
				break;
			default:
				throw new IllegalStateException("unknown state:" + psyncState);
		}
	}
	
	@Override
	protected RESPONSE_STATE doHandleResponse(CmdContext cmdContext, ByteBuf byteBuf) throws XpipeException {
		
		switch(psyncState){
		
			case PSYNC_COMMAND_WAITING_REPONSE:
				Object response = super.readResponse(byteBuf);
				if(response == null){
					return null;
				}
				handleRedisResponse((String)response);
				break;
				
			case READING_RDB:
				if(rdbReader == null){
					rdbReader = new BulkStringParser(new InOutPayloadReplicationStore(currentReplicationStore), this);
				}
				RedisClientProtocol<InOutPayload> payload =  rdbReader.read(byteBuf);
				if( payload != null){
					psyncState = PSYNC_STATE.READING_COMMANDS;
					endReadRdb();
				}else{
					break;
				}
			case READING_COMMANDS:
				try {
					@SuppressWarnings("unused")
					int n = currentReplicationStore.appendCommands(byteBuf);
				} catch (IOException e) {
					logger.error("[doHandleResponse][write commands error]", e);
				}
				break;
			default:
				throw new IllegalStateException("unknown state:" + psyncState);
		}
		
		return RESPONSE_STATE.GO_ON_READING_BUF;
	}


	private void beginReadRdb(long fileSize) {

		for(PsyncObserver observer : observers){
			try{
				observer.beginWriteRdb();
			}catch(Throwable th){
				logger.error("[beginReadRdb]" + this, th);
			}
		}

		try {
			if(logger.isInfoEnabled()){
				logger.info("[beginReadRdb]" + this + "," + fileSize);
			}
			currentReplicationStore.beginRdb(masterRunid, offset, fileSize);
		} catch (IOException e) {
			logger.error("[beginReadRdb]" + masterRunid + "," + offset, e);
		}
	}

	private void endReadRdb() {
		
		try {
			currentReplicationStore.endRdb();
		} catch (IOException e) {
			logger.error("[endReadRdb]", e);
		}
		
		for(PsyncObserver observer : observers){
			try{
				observer.endWriteRdb();
			}catch(Throwable th){
				logger.error("[endReadRdb]" + this, th);
			}
		}
	}

	protected void handleRedisResponse(String psync) {
		
		if(logger.isInfoEnabled()){
			logger.info("[handleResponse]" + psync);
		}
		String []split = splitSpace(psync);
		if(split.length == 0){
			throw new RedisRuntimeException("wrong reply:" + psync);
		}
		
		if(split[0].equalsIgnoreCase(FULL_SYNC)){
			if(split.length != 3){
				throw new RedisRuntimeException("unknown reply:" + psync);
			}
			masterRunid = split[1];
			offset = Long.parseLong(split[2]);
			if(logger.isInfoEnabled()){
				logger.info("[readRedisResponse]" + masterRunid + "," + offset);
			}
			psyncState = PSYNC_STATE.READING_RDB;
			
			
			if(currentReplicationStore== null || !currentReplicationStore.isFresh()){
				
				logger.info("[handleResponse][full sync][replication store out of time, destroy]{}", currentReplicationStore);
				
				ReplicationStore oldStore = currentReplicationStore;
				long newKeeperBeginOffset = 2;
				if(oldStore != null){
					try {
						oldStore.close();
					} catch (IOException e) {
						logger.error("[handleRedisReponse]" + oldStore, e);
					}
					newKeeperBeginOffset = oldStore.getKeeperBeginOffset() + (oldStore.endOffset() - oldStore.beginOffset()) + 1;
					oldStore.delete();
				}
				currentReplicationStore = createNewReplicationStore();
				currentReplicationStore.setKeeperBeginOffset(newKeeperBeginOffset);
				notifyReFullSync();
			}
		}else if(split[0].equalsIgnoreCase(PARTIAL_SYNC)){
			
			psyncState = PSYNC_STATE.READING_COMMANDS;
		}else{
			throw new RedisRuntimeException("unknown reply:" + psync);
		}
	}

	private void notifyReFullSync() {
		
		for(PsyncObserver observer : observers){
			observer.reFullSync();
		}
	}


	private ReplicationStore createNewReplicationStore() {
		
		try {
			return replicationStoreManager.create();
		} catch (IOException e) {
			throw new XpipeRuntimeException("[createNewReplicationStore]" + replicationStoreManager, e);
		}
	}

	@Override
	public void onGotLengthFiled(long length) {
		beginReadRdb(length);
	}
	
	@Override
	protected void doReset() {
		psyncState = PSYNC_STATE.PSYNC_COMMAND_WAITING_REPONSE;
	}
}
