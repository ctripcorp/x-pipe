package com.ctrip.xpipe.redis.server.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author wenchao.meng
 *
 * 2016年4月15日 下午3:01:03
 */
public class Server {
	
	protected Logger logger = LogManager.getLogger(getClass());

	private int port;
	private IoActionFactory ioActionFactory;
	private ExecutorService executors = Executors.newCachedThreadPool();
	
	public Server(int port, IoActionFactory ioActionFactory){
		this.port = port;
		this.ioActionFactory = ioActionFactory;
	}
	
	
	public void start(){
		
		executors.execute(new Runnable() {
			
			@Override
			public void run() {
				
				ServerSocket ss;
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
						IoAction ioAction = ioActionFactory.createIoAction();
						if(ioAction instanceof SocketAware){
							((SocketAware) ioAction).setSocket(socket);
						}
						executors.execute(new Task(socket, ioAction));
					}
					
				} catch (IOException e) {
					logger.error("[run]" + port, e);
				}
			}
		});
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
			}
			
			
		}
	}

}
