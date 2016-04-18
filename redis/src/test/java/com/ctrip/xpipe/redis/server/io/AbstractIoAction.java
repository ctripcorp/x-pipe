package com.ctrip.xpipe.redis.server.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author wenchao.meng
 *
 * 2016年4月15日 下午2:59:11
 */
public abstract class AbstractIoAction implements IoAction{

	protected Logger logger = LogManager.getLogger(getClass());
	
	@Override
	public Object read(InputStream ins) throws IOException {
		
		return doRead(ins);
	}

	protected abstract Object doRead(InputStream ins) throws IOException;

	@Override
	public void write(OutputStream ous) throws IOException {
		
		doWrite(ous);
	}

	protected abstract void doWrite(OutputStream ous) throws IOException;

}
