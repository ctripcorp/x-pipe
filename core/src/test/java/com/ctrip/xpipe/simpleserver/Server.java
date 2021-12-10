package com.ctrip.xpipe.simpleserver;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
	private AtomicInteger totalConnected = new AtomicInteger(0);
	
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
	
	public int getTotalConnected() {
		return totalConnected.get();
	}
	
	@Override
	protected void doStart() throws Exception {
		
		final CountDownLatch latch = new CountDownLatch(1);
		executors.execute(new Runnable() {
			
			@Override
			public void run() {
				
				try {
					try{
						ss = new ServerSocket(port);
						if(logger.isInfoEnabled()){
							logger.info("[run][listening]" + port);
						}
					}finally{
						latch.countDown();
					}
					
					while(true){
						
						Socket socket = ss.accept();
						connected.incrementAndGet();
						totalConnected.incrementAndGet();
						if(logger.isInfoEnabled()){
							logger.info("[run][new socket]" + socket);
						}
						IoAction ioAction = ioActionFactory.createIoAction(socket);
						executors.execute(new Task(socket, ioAction));
					}
					
				} catch (IOException e) {
					logger.warn("[run]" + port + "," + e.getMessage());
				} finally {
					logger.info("[run][stop listening]:{}", ss);
				}
			}
		});
		latch.await(10, TimeUnit.SECONDS);
	}

	@Override
	protected void doStop() throws Exception {
		logger.info("[run][stop server]{}, connected:{}, totalConnected:{} ", ss, connected, totalConnected);
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
				while(true){
					Object read = ioAction.read();
					if(read == null){
						break;
					}
					ioAction.write(read);
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
				} finally {
					logger.info("[run][read and wirte done]");
				}
			}
		}
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + port + ", phase:" + super.getLifecycleState().getPhaseName();
	}
}
