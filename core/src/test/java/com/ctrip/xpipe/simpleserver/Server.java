package com.ctrip.xpipe.simpleserver;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;

import org.slf4j.Logger;

/**
 * @author wenchao.meng
 *
 * 2016年4月15日 下午3:01:03
 */
public class Server extends AbstractLifecycle{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

	private int port;
	private IoActionFactory ioActionFactory;
	private ExecutorService executors = Executors.newCachedThreadPool();
	private ServerSocket ss;
	private AtomicInteger connected = new AtomicInteger(0);
	
	public Server(int port, IoActionFactory ioActionFactory){
		this.port = port;
		this.ioActionFactory = ioActionFactory;
	}
	
	public int getPort() {
		return port;
	}
	
	public int getConnected() {
		return connected.get();
	}
	
	@Override
	protected void doStart() throws Exception {
		executors.execute(new Runnable() {
			
			@Override
			public void run() {
				
				try {
					ss = new ServerSocket(port);
					if(logger.isInfoEnabled()){
						logger.info("[run][listening]" + port);
					}
					while(true){
						
						Socket socket = ss.accept();
						if(logger.isInfoEnabled()){
							logger.info("[run][new socket]" + socket);
						}
						connected.incrementAndGet();
						IoAction ioAction = ioActionFactory.createIoAction();
						if(ioAction instanceof SocketAware){
							((SocketAware) ioAction).setSocket(socket);
						}
						executors.execute(new Task(socket, ioAction));
					}
					
				} catch (IOException e) {
					logger.warn("[run]" + port + "," + e.getMessage());
				}
			}
		});
	}

	@Override
	protected void doStop() throws Exception {
		if(ss != null){
			ss.close();
		}
	}
	
	
	public class Task implements Runnable{
		
		private Socket socket;
		private IoAction ioAction;
		
		public Task(Socket socket, IoAction ioAction){
			this.socket = socket;
			this.ioAction = ioAction;
		}
		
		@Override
		public void run() {
			
			try {
				InputStream ins = socket.getInputStream();
				OutputStream ous = socket.getOutputStream();
				
				while(true){
					Object read = ioAction.read(ins);
					if(read == null){
						break;
					}
					ioAction.write(ous);
				}
			} catch (IOException e) {
				logger.error("[run]" + socket, e);
			}finally{
				try {
					if(ioAction instanceof DeadAware){
						((DeadAware) ioAction).setDead();
					}
					connected.decrementAndGet();
					socket.close();
				} catch (IOException e) {
					logger.error("[close]", e);
				}
			}
		}
	}

}
