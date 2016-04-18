package com.ctrip.xpipe.redis.server.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author wenchao.meng
 *
 * 2016年4月15日 下午2:58:27
 */
public interface IoAction {

	
	Object read(InputStream ins) throws IOException;
	
	void write(OutputStream ous) throws IOException;
}
