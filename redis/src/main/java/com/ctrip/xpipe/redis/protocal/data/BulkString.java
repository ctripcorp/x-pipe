package com.ctrip.xpipe.redis.protocal.data;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.ctrip.xpipe.redis.exception.RedisRuntimeException;


/**
 * @author wenchao.meng
 *
 * 2016年3月28日 下午2:35:36
 */
public class BulkString extends AbstractRedisClientProtocol<InputStream>{
	
	private OutputStream ousForBulk;
	
	public BulkString(InputStream payload){
		super(payload);
		
	}
	
	public BulkString(OutputStream ousForBulk) {
		this.ousForBulk = ousForBulk;
	}

	/**
	 * 返回值暂时没用
	 */
	@Override
	public InputStream parse(InputStream ins) throws IOException {
		
		Long length = readLengthFiled(ins);
		
		if(logger.isInfoEnabled()){
			logger.info("[parse][length]" + length);
		}
		if(length == -1L){
			return null;
		}
		
		for(long i=0; i < length; i++){
			
			int data = ins.read();
			if(data == -1){
				throw new RedisRuntimeException("[parse][eof found, but we have not got enough bytes]" + length);
			}
			ousForBulk.write(data);
		}
		
		return null;
	}

	
	private Long readLengthFiled(InputStream ins) throws IOException {
		
		String lengthStr = readTilCRLFAsString(ins);
		lengthStr = lengthStr.trim();
		if(lengthStr.charAt(0) == DOLLAR_BYTE){
			lengthStr = lengthStr.substring(1);
		}
		return Long.parseLong(lengthStr);
	}

	@Override
	protected void doWrite(OutputStream ous) throws IOException {
		throw new UnsupportedOperationException();		
	}

}
