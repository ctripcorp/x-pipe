package com.ctrip.xpipe.simpleserver;


import java.io.IOException;
/**
 * @author wenchao.meng
 *
 * 2016年4月15日 下午2:58:27
 */
public interface IoAction extends SocketAware{

	Object read() throws IOException;
	
	void write(Object readResult) throws IOException;
}
