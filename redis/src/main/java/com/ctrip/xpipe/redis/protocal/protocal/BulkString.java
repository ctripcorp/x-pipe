package com.ctrip.xpipe.redis.protocal.protocal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.redis.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.protocal.RedisClietProtocol;


/**
 * @author wenchao.meng
 *
 * 2016年3月28日 下午2:35:36
 */
public class BulkString extends AbstractRedisClientProtocol<InOutPayload>{
	
	public BulkString(){
	}
	
	public BulkString(InOutPayload bulkStringPayload) {
		super(bulkStringPayload, false, false);
	}

	/**
	 * 返回值暂时没用
	 */
	@Override
	public RedisClietProtocol<InOutPayload> parse(InputStream ins) throws IOException {
		
		Long length = readLengthFiled(ins);
		
		if(logger.isInfoEnabled()){
			logger.info("[parse][length]" + length);
		}
		if(length == -1L){
			return null;
		}
		
		OutputStream ous = payload.getOutputStream();
		
		for(long i=0; i < length; i++){
			
			int data = ins.read();
			if(data == -1){
				throw new RedisRuntimeException("[parse][eof found, but we have not got enough bytes]" + length);
			}
			ous.write(data);
		}
		ous.flush();
		return new BulkString(payload);
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
	protected byte[] getWriteBytes() {
		throw new UnsupportedOperationException();		
	}
	
}
