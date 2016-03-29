package com.ctrip.xpipe.redis.protocal.cmd;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.ctrip.xpipe.redis.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.protocal.AbstractRedisCommand;
import com.ctrip.xpipe.redis.protocal.data.BulkString;
import com.ctrip.xpipe.redis.protocal.data.SimpleString;
import com.ctrip.xpipe.redis.rdb.RdbWriter;


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
	
	private RdbWriter rdbWriter;
	
	private OutputStream commandOus;

	protected Psync(OutputStream ous, InputStream ins, RdbWriter rdbWriter, OutputStream commandOus) {
		this(ous, ins, "?", -1L, rdbWriter, commandOus);
	}

	public Psync(OutputStream ous, InputStream ins, String masterRunId, long offset, RdbWriter rdbWriter, OutputStream commandOus) {
		super(ous, ins);
		this.masterRunIdRequest = masterRunId;
		this.offsetRequest = offset;
		this.rdbWriter = rdbWriter;
		this.commandOus = commandOus;
	}
	
	
	@Override
	public String getName() {
		return "psync";
	}


	@Override
	protected void doRequest() throws IOException {
		writeAndFlush(getName(), masterRunIdRequest, String.valueOf(offsetRequest));
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
		return getName() + " " + masterRunIdRequest + " " + offsetRequest + "";
	}

	@Override
	protected void readRedisResponse(int sign) throws IOException {

		String psync = new SimpleString().parse(ins).trim();
		
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
			fullSync();
		}else if(split[0].equalsIgnoreCase(PARTIAL_SYNC)){
			isFull = false;
		}else{
			throw new RedisRuntimeException("unknown reply:" + psync);
		}

	}

	private void fullSync() throws IOException {
		
		//read rdb
		try{
			rdbWriter.beginWrite();
			new BulkString(rdbWriter).parse(ins);
		}finally{
			rdbWriter.endWrite();
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
			}
		}else{
			commandOus.write(data);
		}
		while(true){
			data = ins.read();
			if(data == -1){
				logger.error("[readPropogateCommands][EOF]");
				break;
			}
			
			commandOus.write(data);
		}
		
	}
}
