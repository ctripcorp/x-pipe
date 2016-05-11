package com.ctrip.xpipe.redis.protocal.cmd;


import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.redis.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.keeper.ReplicationStore;
import com.ctrip.xpipe.redis.protocal.PsyncObserver;
import com.ctrip.xpipe.redis.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.protocal.protocal.BulkStringParser;
import com.ctrip.xpipe.redis.protocal.protocal.BulkStringParser.BulkStringParserListener;
import com.ctrip.xpipe.redis.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.redis.protocal.protocal.SimpleStringParser;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;


/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午2:24:38
 */
public class Psync extends AbstractRedisCommand implements BulkStringParserListener{
	
	
	public static final String FULL_SYNC = "FULLRESYNC";
	public static final String PARTIAL_SYNC = "CONTINUE";
	
	
	private final String masterRunidRequest; 
	private final long offsetRequest; 

	private String masterRunid;
	private long offset;
	
	private boolean isFull;
	
	private BulkStringParser rdbReader;
	
	private ReplicationStore replicationStore;

	private List<PsyncObserver> observers = new LinkedList<PsyncObserver>();
	
	private PSYNC_STATE psyncState = PSYNC_STATE.PSYNC_COMMAND_WAITING_REPONSE; 
	
	public static enum PSYNC_STATE{
		PSYNC_COMMAND_WAITING_REPONSE,
		READING_RDB,
		READING_COMMANDS
	}
	
	
	public Psync(Channel channel, ReplicationStore replicationStore) {
		this(channel, "?", -1L, replicationStore);
	}

	public Psync(Channel channel, String masterRunid, long offset, ReplicationStore replicationStore) {
		super(channel);
		this.masterRunidRequest = masterRunid;
		this.offsetRequest = offset;
		this.replicationStore = replicationStore;
	}
	
	
	@Override
	public String getName() {
		return "psync";
	}
	
	
	public void addPsyncObserver(PsyncObserver observer){
		this.observers.add(observer);
	}


	@Override
	protected void doRequest() {
		RequestStringParser requestString = new RequestStringParser(getName(), masterRunidRequest, String.valueOf(offsetRequest));
		writeAndFlush(requestString.format());
	}


	public String getMasterRunId() {
		return masterRunid;
	}
	
	public long getOffset() {
		return offset;
	}

	public boolean isFull() {
		return isFull;
	}
	
	@Override
	public String toString() {
		
		String info = getName() + "," + masterRunidRequest + "," + offsetRequest; 
		if(!masterRunidRequest.equals(masterRunid)){
			info += "," + masterRunid;
		}
		return info;
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
	protected RESPONSE_STATE doHandleResponse(ByteBuf byteBuf) throws XpipeException {
		
		switch(psyncState){
		
			case PSYNC_COMMAND_WAITING_REPONSE:
				return super.doHandleResponse(byteBuf);
				
			case READING_RDB:
				if(rdbReader == null){
					rdbReader = new BulkStringParser(new InOutPayloadReplicationStore(replicationStore), this);
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
					int n = replicationStore.appendCommands(byteBuf);
				} catch (IOException e) {
					logger.error("[doHandleResponse][write commands error]", e);
				}
				break;
			default:
				throw new IllegalStateException("unknown state:" + psyncState);
		}
		
		return RESPONSE_STATE.CONTINUE;
	}


	private void beginReadRdb(long fileSize) {
		
		try {
			if(logger.isInfoEnabled()){
				logger.info("[beginReadRdb]" + this + "," + fileSize);
			}
			replicationStore.beginRdb(masterRunid, offset, fileSize);
		} catch (IOException e) {
			logger.error("[beginReadRdb]" + masterRunid + "," + offset, e);
		}
		for(PsyncObserver observer : observers){
			try{
				observer.beginWriteRdb();
			}catch(Throwable th){
				logger.error("[beginReadRdb]" + this, th);
			}
		}
	}

	private void endReadRdb() {
		
		try {
			replicationStore.endRdb();
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

	@Override
	protected RESPONSE_STATE handleRedisResponse(RedisClientProtocol<?> redisClietProtocol) {
		
		String psync = ((SimpleStringParser)redisClietProtocol).getPayload();
		
		String []split = splitSpace(psync);
		if(split.length == 0){
			throw new RedisRuntimeException("wrong reply:" + psync);
		}
		
		if(split[0].equalsIgnoreCase(FULL_SYNC)){
			isFull = true;
			if(split.length != 3){
				throw new RedisRuntimeException("unknown reply:" + psync);
			}
			masterRunid = split[1];
			offset = Long.parseLong(split[2]);
			if(logger.isInfoEnabled()){
				logger.info("[readRedisResponse]" + masterRunid + "," + offset);
			}
			psyncState = PSYNC_STATE.READING_RDB;
		}else if(split[0].equalsIgnoreCase(PARTIAL_SYNC)){
			psyncState = PSYNC_STATE.READING_COMMANDS;
		}else{
			throw new RedisRuntimeException("unknown reply:" + psync);
		}
		return RESPONSE_STATE.CONTINUE;
	}

	@Override
	public void onGotLengthFiled(long length) {
		beginReadRdb(length);
	}
}
