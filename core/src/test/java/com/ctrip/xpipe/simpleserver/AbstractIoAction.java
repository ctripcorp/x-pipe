package com.ctrip.xpipe.simpleserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * 2016年4月15日 下午2:59:11
 */
public abstract class AbstractIoAction implements IoAction, DeadAware{

	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	protected ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(4);
	
	protected Socket socket; 
	
	public AbstractIoAction(Socket socket) {
		this.socket = socket;
	}

	@Override
	public Object read() throws IOException {
		
		return doRead(socket.getInputStream());
	}

	protected abstract Object doRead(InputStream ins) throws IOException;

	@Override
	public void write(Object readResult) throws IOException {
		
		doWrite(socket.getOutputStream(), readResult);
	}

	protected abstract void doWrite(OutputStream ous, Object readResult) throws IOException;
	
	
	public static String readLine(InputStream ins) throws IOException {
		
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
