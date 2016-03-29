package com.ctrip.xpipe.api.payload;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author wenchao.meng
 *
 * 2016年3月29日 下午4:30:45
 */
public interface InOutPayload {


	void startInputStream();
	
	InputStream getInputStream();
	
	void endInputStream();
	

	void startOutputStream();
	
	OutputStream getOutputStream();
	
	void endOutputStream();
}
