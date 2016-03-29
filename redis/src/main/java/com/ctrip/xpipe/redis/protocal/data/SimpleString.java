package com.ctrip.xpipe.redis.protocal.data;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午6:31:56
 */
public class SimpleString extends AbstractRedisClientProtocol<String>{

	public SimpleString() {
	}
	
	public SimpleString(String payload) {
		super(payload);
	}
	
	@Override
	public String parse(InputStream ins) throws IOException {
		
		return readTilCRLFAsString(ins);
	}

	@Override
	protected void doWrite(OutputStream ous) throws IOException {
		write(ous, getRequestBytes(PLUS_BYTE, payload));
	}


}
