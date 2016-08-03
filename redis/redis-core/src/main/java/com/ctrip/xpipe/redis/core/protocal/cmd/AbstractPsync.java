package com.ctrip.xpipe.redis.core.protocal.cmd;


import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.PsyncObserver;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.BulkStringParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.BulkStringParser.BulkStringParserListener;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.utils.StringUtil;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;


/**
 * @author marsqing
 *
 * 2016年3月24日 下午2:24:38
 */
public abstract class AbstractPsync extends AbstractRedisCommand<Object> implements BulkStringParserListener{
	
	
	public static final String FULL_SYNC = "FULLRESYNC";
	public static final String PARTIAL_SYNC = "CONTINUE";
		
	private BulkStringParser rdbReader;
	
	protected volatile ReplicationStore  	    currentReplicationStore;
	
	private String masterRunid;
	private long   offset;
	
	private List<PsyncObserver> observers = new LinkedList<PsyncObserver>();
	
	private PSYNC_STATE psyncState = PSYNC_STATE.PSYNC_COMMAND_WAITING_REPONSE;
	
	private boolean saveCommands;
	
	public static enum PSYNC_STATE{
		PSYNC_COMMAND_WAITING_REPONSE,
		READING_RDB,
		READING_COMMANDS
	}
	
	
	public AbstractPsync(SimpleObjectPool<NettyClient> clientPool, boolean saveCommands) {
		super(clientPool);
		this.saveCommands = saveCommands;
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
			masterRunidRequest = currentReplicationStore.getMetaStore().getMasterRunid();
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

	protected abstract ReplicationStore getCurrentReplicationStore();

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
	protected Object doReceiveResponse(Channel channel, ByteBuf byteBuf) throws Exception {
		
		switch(psyncState){
		
			case PSYNC_COMMAND_WAITING_REPONSE:
				Object response = super.doReceiveResponse(channel, byteBuf);
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
					
					if(!saveCommands) {
						try {
							channel.close();
						} catch(Exception e) {
							// ignore it
						}
					}
				}else{
					break;
				}
			case READING_COMMANDS:
				if(saveCommands) {
					try {
						@SuppressWarnings("unused")
						int n = currentReplicationStore.getCommandStore().appendCommands(byteBuf);
					} catch (IOException e) {
						logger.error("[doHandleResponse][write commands error]" + this, e);
					}
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
			currentReplicationStore.getRdbStore().endRdb();
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

	protected void handleRedisResponse(String psync) throws IOException {
		
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
			
			doWhenFullSync(masterRunid);
		}else if(split[0].equalsIgnoreCase(PARTIAL_SYNC)){
			
			psyncState = PSYNC_STATE.READING_COMMANDS;
			notifyContinue();
		}else{
			throw new RedisRuntimeException("unknown reply:" + psync);
		}
	}

	protected abstract void doWhenFullSync(String masterRunid) throws IOException;

	private void notifyContinue() {
		
		for(PsyncObserver observer : observers){
			observer.onContinue();
		}
	}


	protected void notifyReFullSync() {
		
		for(PsyncObserver observer : observers){
			observer.reFullSync();
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
