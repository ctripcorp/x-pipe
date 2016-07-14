package com.ctrip.xpipe.redis.core.protocal.cmd;


import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.PsyncObserver;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.BulkStringParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.BulkStringParser.BulkStringParserListener;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import com.ctrip.xpipe.utils.StringUtil;

import io.netty.buffer.ByteBuf;


/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午2:24:38
 */
public class Psync extends AbstractRedisCommand<Object> implements BulkStringParserListener{
	
	
	public static final String FULL_SYNC = "FULLRESYNC";
	public static final String PARTIAL_SYNC = "CONTINUE";
		
	private BulkStringParser rdbReader;
	
	private ReplicationStoreManager replicationStoreManager;
	private volatile ReplicationStore  	    currentReplicationStore;
	
	private String masterRunid;
	private long   offset;
	
	private KeeperMeta keeperMeta;

	private List<PsyncObserver> observers = new LinkedList<PsyncObserver>();
	
	private PSYNC_STATE psyncState = PSYNC_STATE.PSYNC_COMMAND_WAITING_REPONSE;
	
	private Endpoint masterEndPoint;
	
	public static enum PSYNC_STATE{
		PSYNC_COMMAND_WAITING_REPONSE,
		READING_RDB,
		READING_COMMANDS
	}
	
	
	public Psync(SimpleObjectPool<NettyClient> clientPool, 
			Endpoint masterEndPoint, KeeperMeta keeperMeta, ReplicationStoreManager replicationStoreManager) {
		super(clientPool);
		this.masterEndPoint = masterEndPoint;
		this.keeperMeta = keeperMeta;
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
	protected ByteBuf getRequest() {
		
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
		logger.info("[doRequest]{}, {}", this, StringUtil.join(" ", requestString.getPayload()));
		return requestString.format();
	}

	private ReplicationStore getCurrentReplicationStore() {
		
		try {
			return replicationStoreManager.getCurrent();
		} catch (IOException e) {
			logger.error("[doRequest]" + this + replicationStoreManager, e);
			throw new XpipeRuntimeException("[doRequest]getReplicationStore failed." + replicationStoreManager, e);
		}
	}


	@Override
	public String toString() {
		return getName() + "->"  + masterEndPoint;
	}
	
	@Override
	public void clientClosed(NettyClient nettyClient) {
		
		super.clientClosed(nettyClient);
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
	protected Object doReceiveResponse(ByteBuf byteBuf) throws Exception {

		switch(psyncState){
		
			case PSYNC_COMMAND_WAITING_REPONSE:
				Object response = super.doReceiveResponse(byteBuf);
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
					logger.error("[doHandleResponse][write commands error]" + this, e);
				}
				break;
			default:
				throw new IllegalStateException("unknown state:" + psyncState);
		}
		
		return null;
	}


	private void beginReadRdb(long fileSize) {

		if(logger.isInfoEnabled()){
			logger.info("[beginReadRdb]" + this + "," + fileSize);
		}

		for(PsyncObserver observer : observers){
			try{
				observer.beginWriteRdb();
			}catch(Throwable th){
				logger.error("[beginReadRdb]" + this, th);
			}
		}

		try {
			currentReplicationStore.beginRdb(masterRunid, offset, fileSize);
		} catch (IOException e) {
			logger.error("[beginReadRdb]" + masterRunid + "," + offset, e);
		}
	}

	private void endReadRdb() {
		
		logger.info("[endReadRdb]{}", this);
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
			logger.info("[handleResponse]{}, {}" , this, psync);
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
				logger.info("[readRedisResponse]{},{},{}", this, masterRunid, offset);
			}
			psyncState = PSYNC_STATE.READING_RDB;
			
			
			if(currentReplicationStore== null || !currentReplicationStore.isFresh()){
				
				logger.info("[handleResponse][full sync][replication store out of time, destroy]{}, {}", this, currentReplicationStore);
				
				ReplicationStore oldStore = currentReplicationStore;
				long newKeeperBeginOffset = ReplicationStoreMeta.DEFAULT_KEEPER_BEGIN_OFFSET;
				if(oldStore != null){
					try {
						oldStore.close();
					} catch (IOException e) {
						logger.error("[handleRedisReponse]" + oldStore, e);
					}
					newKeeperBeginOffset = oldStore.getKeeperBeginOffset() + (oldStore.endOffset() - oldStore.beginOffset()) + 2;
					oldStore.delete();
				}
				currentReplicationStore = createIfDirtyOrNotExist();
				logger.info("[handleRedisResponse][set keepermeta]{}, {}", keeperMeta.getId(), newKeeperBeginOffset);
				currentReplicationStore.setKeeperMeta(keeperMeta.getId(), newKeeperBeginOffset);
				notifyReFullSync();
			}
		}else if(split[0].equalsIgnoreCase(PARTIAL_SYNC)){
			
			psyncState = PSYNC_STATE.READING_COMMANDS;
			notifyContinue();
		}else{
			throw new RedisRuntimeException("unknown reply:" + psync);
		}
	}

	private void notifyContinue() {
		
		for(PsyncObserver observer : observers){
			observer.onContinue();
		}
	}


	private void notifyReFullSync() {
		
		for(PsyncObserver observer : observers){
			observer.reFullSync();
		}
	}


	private ReplicationStore createIfDirtyOrNotExist() {
		
		try {
			return replicationStoreManager.createIfDirtyOrNotExist();
		} catch (IOException e) {
			throw new XpipeRuntimeException("[createNewReplicationStore]" + replicationStoreManager, e);
		}
	}

	@Override
	public void onGotLengthFiled(long length) {
		beginReadRdb(length);
	}


	@Override
	protected Object format(Object payload) {
		return payload;
	}
	
	@Override
	protected void doReset() {
		throw new UnsupportedOperationException("not supported");
	}
}
