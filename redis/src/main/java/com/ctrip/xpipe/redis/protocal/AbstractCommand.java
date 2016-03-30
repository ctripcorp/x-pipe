package com.ctrip.xpipe.redis.protocal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ctrip.xpipe.exception.XpipeException;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午12:04:13
 */
public abstract class AbstractCommand implements Command{
	
	protected Logger logger = LogManager.getLogger();

	protected OutputStream ous;
	protected InputStream  ins;
	
	protected Charset charset = Charset.forName("UTF-8");
	
	protected AbstractCommand(OutputStream ous, InputStream ins){
		
		this.ous = ous;
		this.ins = ins;
	}
	@Override
	public void request() throws IOException, XpipeException {
		doRequest();
		if(hasResponse()){
			readResponse();
		}
	}
	
	protected boolean hasResponse() {
		return true;
	}

	protected abstract void doRequest() throws IOException;


	public void readResponse() throws XpipeException, IOException  {
		doReadResponse();
	}

	protected abstract void doReadResponse() throws XpipeException, IOException;
}
