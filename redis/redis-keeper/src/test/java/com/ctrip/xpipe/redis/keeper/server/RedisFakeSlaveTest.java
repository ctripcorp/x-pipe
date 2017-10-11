package com.ctrip.xpipe.redis.keeper.server;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.simpleserver.IoAction;
import com.ctrip.xpipe.simpleserver.IoActionFactory;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *
 * 2016年4月15日 下午2:53:55
 */
public class RedisFakeSlaveTest extends AbstractRedisKeeperTest{
	
	private String redisMasterUri = "redis://127.0.0.1:6379";
	private int fakeSlavePort = 8888;
	private Socket socket;

	@Test
	public void simpleTest(){
		
		logger.info("hello {} {}", new Object[]{"nihao", 1234});
		logger.info("hello {} {}", "nihao", 1234);
	}
	
	
	@Test
	public void testKeeperCreateReplicationLog() throws Exception{
		
		startFakeSlaveServer();
		startKeeperCreateReplicationLogClient();
	}

	private void startFakeSlaveServer() throws Exception {
		
		new Server(fakeSlavePort, new IoActionFactory() {
			
			@Override
			public IoAction createIoAction(Socket socket) {
				return new AbstractRedisSlaveAction(socket) {
					
					@Override
					protected String getInfo() {
						
						Endpoint endpoint = new DefaultEndPoint(redisMasterUri);
						//simple information to be added
						return 
								"role:slave\r\n" + 
								"master_host:" + endpoint.getHost() + "\r\n" + 
								"master_port:"+ endpoint.getPort() + "\r\n" +
								"slave_priority:0";
					}
				};
			}
		}).start();
	}

	private void startKeeperCreateReplicationLogClient() throws UnknownHostException, IOException {

		try{
			Endpoint endpoint = new DefaultEndPoint(redisMasterUri);
			
			socket = new Socket(endpoint.getHost(), endpoint.getPort());
			if(logger.isInfoEnabled()){
				logger.info("[startKeeperClient]" + socket);
			}
			final OutputStream ous = socket.getOutputStream();
			InputStream ins = socket.getInputStream();
			
			ous.write(("replconf listening-port "+ fakeSlavePort + "\r\n").getBytes());
			ous.flush();
			readLine(ins);
	
			ous.write("fsync\r\n".getBytes());
			ous.flush();
			readLine(ins);
			
			
			scheduled.scheduleAtFixedRate(new Runnable(){
	
				@Override
				public void run() {
					try {
						ous.write("replconf ack 0\r\n".getBytes());
						ous.flush();
					} catch (IOException e) {
						closeSocket();
						logger.error("[run]", e);
					}
				}
				
			}, 0, 1, TimeUnit.SECONDS);
		}catch(IOException e){
			closeSocket();
			logger.error("[startKeeperCreateReplicationLogClient]" + socket, e);
		}
	}

	
	private void closeSocket() {
		if(socket != null){
			try {
				socket.close();
			} catch (IOException e) {
				logger.error("[closeSocket]" + socket, e);
			}
		}
	}

	@After
	public void afterRedisFakeSlaveTest() throws InterruptedException{
		TimeUnit.SECONDS.sleep(1000);
	}

}
