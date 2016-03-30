package com.ctrip.xpipe.redis.protocal.cmd;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.redis.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.protocal.AbstractRedisCommand;
import com.ctrip.xpipe.redis.protocal.PsyncObserver;
import com.ctrip.xpipe.redis.protocal.RedisClietProtocol;
import com.ctrip.xpipe.redis.protocal.data.BulkString;
import com.ctrip.xpipe.redis.protocal.data.RequestString;
import com.ctrip.xpipe.redis.protocal.data.SimpleString;


/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午2:24:38
 */
public class Psync extends AbstractRedisCommand{
	
	
	public static final String FULL_SYNC = "FULLRESYNC";
	public static final String PARTIAL_SYNC = "CONTINUE";
	
	
	private final String masterRunIdRequest; 
	private final long offsetRequest; 

	private String masterRunId;
	private long offset;
	
	private boolean isFull;
	
	private InOutPayload rdbPayload;
	
	private OutputStream commandOus;

	private List<PsyncObserver> observers = new LinkedList<PsyncObserver>();
	
	
	protected Psync(OutputStream ous, InputStream ins, InOutPayload rdbPayload, OutputStream commandOus) {
		this(ous, ins, "?", -1L, rdbPayload, commandOus);
	}

	public Psync(OutputStream ous, InputStream ins, String masterRunId, long offset, InOutPayload rdbPayload, OutputStream commandOus) {
		super(ous, ins);
		this.masterRunIdRequest = masterRunId;
		this.offsetRequest = offset;
		this.rdbPayload = rdbPayload;
		this.commandOus = commandOus;
	}
	
	
	@Override
	public String getName() {
		return "psync";
	}
	
	
	public void addPsyncObserver(PsyncObserver observer){
		this.observers.add(observer);
	}


	@Override
	protected void doRequest() throws IOException {
		RequestString requestString = new RequestString(getName(), masterRunIdRequest, String.valueOf(offsetRequest));
		requestString.write(ous);
	}


	public String getMasterRunId() {
		return masterRunId;
	}
	
	public long getOffset() {
		return offset;
	}

	public boolean isFull() {
		return isFull;
	}
	
	@Override
	public String toString() {
		
		String info = getName() + "," + masterRunIdRequest + "," + offsetRequest; 
		if(!masterRunIdRequest.equals(masterRunId)){
			info += "," + masterRunId;
		}
		return info;
	}

	private void fullSync() throws IOException {
		
		//read rdb
		try{
			beginReadRdb();
			rdbPayload.startOutputStream();
			
			new BulkString(rdbPayload).parse(ins);
		}finally{
			rdbPayload.endOutputStream();
			endReadRdb();
		}

		readPropogateCommands(ins);
		
	}

	private void readPropogateCommands(InputStream ins) throws IOException {
		//处理结尾\r\n的情况
		
		int data = ins.read();
		if(data == '\r'){
			data = ins.read();
			if(data == '\n'){
				
			}else{
				commandOus.write(data);
				notifyIncreaseOffset();
			}
		}else{
			commandOus.write(data);
			notifyIncreaseOffset();
		}
		while(true){
			data = ins.read();
			if(data == -1){
				logger.error("[readPropogateCommands][EOF]");
				break;
			}
			
			commandOus.write(data);
			notifyIncreaseOffset();
		}
		
	}

	private void beginReadRdb() {
		
		for(PsyncObserver observer : observers){
			try{
				observer.beginWriteRdb();
			}catch(Throwable th){
				logger.error("[beginReadRdb]" + this, th);
			}
		}
	}

	private void endReadRdb() {
		for(PsyncObserver observer : observers){
			try{
				observer.endWriteRdb();
			}catch(Throwable th){
				logger.error("[endReadRdb]" + this, th);
			}
		}
	}


	private void notifyFullSync(String masterRunId, long offset) {

		for(PsyncObserver observer : observers){
			try{
				observer.setFullSyncInfo(masterRunId, offset);
			}catch(Throwable th){
				logger.error("[notifyFullSync]" + this, th);
			}
		}
	}

	private void notifyIncreaseOffset() {

		for(PsyncObserver observer : observers){
			try{
				observer.increaseReploffset();
			}catch(Throwable th){
				logger.error("[notifyIncreaseOffset]" + this, th);				
			}
		}
	}

	@Override
	protected void handleRedisResponse(RedisClietProtocol<?> redisClietProtocol) throws IOException {
		
		String psync = ((SimpleString)redisClietProtocol).getPayload();
		
		String []split = splitSpace(psync);
		if(split.length == 0){
			throw new RedisRuntimeException("wrong reply:" + psync);
		}
		
		if(split[0].equalsIgnoreCase(FULL_SYNC)){
			isFull = true;
			if(split.length != 3){
				throw new RedisRuntimeException("unknown reply:" + psync);
			}
			masterRunId = split[1];
			offset = Long.parseLong(split[2]);
			if(logger.isInfoEnabled()){
				logger.info("[readRedisResponse]" + masterRunId + "," + offset);
			}
			notifyFullSync(masterRunId, offset);
			fullSync();
		}else if(split[0].equalsIgnoreCase(PARTIAL_SYNC)){
			isFull = false;
		}else{
			throw new RedisRuntimeException("unknown reply:" + psync);
		}

		
	}

}
