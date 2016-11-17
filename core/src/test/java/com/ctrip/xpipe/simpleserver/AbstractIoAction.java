package com.ctrip.xpipe.simpleserver;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * @author wenchao.meng
 *
 * 2016年4月15日 下午2:59:11
 */
public abstract class AbstractIoAction implements IoAction, SocketAware, DeadAware{

	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	protected ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(4);
	
	protected Socket socket; 

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
	
	
	protected String readLine(InputStream ins) throws IOException {
		
		StringBuilder sb = new StringBuilder();
		int last = 0;
		while(true){
			
			int data = ins.read();
			if(data == -1){
				return null;
			}
			
			sb.append((char)data);
			if(data == '\n' && last == '\r'){
				break;
			}
			last = data;
		}
		
		return sb.toString();
	}

	@Override
	public void setSocket(Socket socket) {
		this.socket = socket;
	}

	public Socket getSocket() {
		return socket;
	}
	
	@Override
	public void setDead() {
		
	}
	
	
	@Override
	public String toString() {
		return String.format("%s(%s)", getClass().getSimpleName(), getSocket());
	}

}
