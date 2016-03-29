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

	protected void writeAndFlush(byte []data) throws IOException{
		
		write(data);
		flush();
	}
	
	protected void write(byte []data) throws IOException {
		
		if(logger.isInfoEnabled()){
			logger.info("[write]" + new String(data, charset).trim());
		}
		ous.write(data);
	}
	
	protected void flush() throws IOException {
		ous.flush();
	}
	
	protected void writeWithCRLFAndFlush(byte []data) throws IOException {
		
		write(data);
		write("\r\n");
		flush();
	}


	protected void writeAndFlush(String buff) throws IOException{
		
		write(buff);
		flush();
	}
	
	protected void write(String buff) throws IOException {
		write(buff, charset);
	}
	
	protected void write(String buff, Charset charset) throws IOException{
		
		write(buff.getBytes(charset));
	}

	@Override
	public void request() throws IOException, XpipeException {
		doRequest();
		readResponse();
	}
	
	protected abstract void doRequest() throws IOException;


	public void readResponse() throws XpipeException, IOException  {
		doReadResponse();
	}

	protected abstract void doReadResponse() throws XpipeException, IOException;
}
