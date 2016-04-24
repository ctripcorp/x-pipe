package com.ctrip.xpipe.redis.server;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.AbstractRedisTest;
import com.ctrip.xpipe.redis.keeper.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import com.ctrip.xpipe.redis.server.io.IoAction;
import com.ctrip.xpipe.redis.server.io.IoActionFactory;
import com.ctrip.xpipe.redis.server.io.Server;
import com.ctrip.xpipe.redis.tools.SimpleFileReplicationStore;

/**
 * @author wenchao.meng
 *
 * 2016年3月30日 上午9:00:35
 */
public class RedisKeeperTest extends AbstractRedisTest{
	

	private String redisMasterUri = "redis://127.0.0.1:6379";


	private String keeperRunid = "XXXXde3e7062ca030d439d60095458265a2eXXXX";
	private KeeperSlaveFactory keeperSlaveFactory;

	//as Slave
	private int keeperPort = 7777;
	private String rdbFile = "/data/xpipe/redis/dump.rdb";
	private String commandFile = "/data/xpipe/redis/command.out";

	//real slaves
	private String slave1Runid = "XXXXde3e7062ca030d439d60095458265aslave1";
	private int slave1Port = 8888;
	private String slave2Runid = "XXXXde3e7062ca030d439d60095458265aslave2";
	private int slave2Port = 9999;
	private SimplePrintSlaveFactory slave1 = new SimplePrintSlaveFactory(slave1Runid, redisMasterUri, 10);
	private SimplePrintSlaveFactory slave2 = new SimplePrintSlaveFactory(slave2Runid, redisMasterUri, 20);
	
	
	@Test
	public void createRedisKeeper() throws Exception{

		startKeeperServer(keeperPort);
		startSlaveClient();
		startSlaves();
	}
	
	private void startSlaves() {
		new Server(slave1Port, slave1).start();
		new Server(slave2Port, slave2).start();
	}

	private void startSlaveClient() throws Exception {

		ReplicationStore replicationStore = new SimpleFileReplicationStore(rdbFile, commandFile);
		DefaultRedisKeeperServer rds = new DefaultRedisKeeperServer(new DefaultEndPoint(redisMasterUri), replicationStore, keeperRunid, keeperPort);
		rds.start();
		
	}
	private void startKeeperServer(int keeperPort) throws IOException {
		
		keeperSlaveFactory = new KeeperSlaveFactory(redisMasterUri, slave1Port, slave2Port);
		new Server(keeperPort, keeperSlaveFactory).start();
	}


	@After
	public void afterRedisKeeperTest() throws InterruptedException{
		TimeUnit.SECONDS.sleep(10000);
	}

	
	@After
	public void afterRedisDataSourceTest(){

		sleepSeconds(10000);
	}
	
	
	private void changeToSlave(List<String> slaveOfCommands) {
		
		if(slaveOfCommands.size() < 4){
			throw new IllegalArgumentException("wrong argument:" + slaveOfCommands);
		}
		
		if(slaveOfCommands.get(1).equalsIgnoreCase("NO")){
			changeToMaster(slaveOfCommands.get(3));
		}else{
			changeMaster(slaveOfCommands.get(1), Integer.valueOf(slaveOfCommands.get(2)), slaveOfCommands.get(3));
		}
	}

	private void changeMaster(String masterIp, Integer masterPort, String destHostPort) {
		
		int port = Integer.parseInt(destHostPort.split(":")[1]);
		if(port == slave1Port){
			slave1.setRedisMasterUri("redis://" + masterIp + ":" + masterPort);
		}
		if(port == slave2Port){
			slave2.setRedisMasterUri("redis://" + masterIp + ":" + masterPort);
		}
		
	}

	private void changeToMaster(String destHostPort) {
		
		int port = Integer.parseInt(destHostPort.split(":")[1]);
		if(port == slave1Port){
			slave1.setRole("master");
		}
		if(port == slave2Port){
			slave2.setRole("master");
		}
		keeperSlaveFactory.changeTomaster(port);
	}

	
	public class KeeperSlaveFactory implements IoActionFactory{
		
		private String redisMasterUri;
		private List<Integer> slaves = new ArrayList<Integer>();
		
		public KeeperSlaveFactory(String redisMasterUri, int ...slavePorts){
			this.redisMasterUri = redisMasterUri;
			for(int slavePort : slavePorts){
				slaves.add(slavePort);
			}
		}
		
		public void changeTomaster(int changeToMasterPort){
			this.redisMasterUri = "redis://127.0.0.1:" + changeToMasterPort;
			Iterator<Integer> iter = slaves.iterator();
			
			while(iter.hasNext()){
				Integer i = iter.next();
				if(i.equals(changeToMasterPort)){
					iter.remove();
				}
			}
			if(logger.isInfoEnabled()){
				logger.info("[changeTomaster]" +slaves);
			}
		}

		@Override
		public IoAction createIoAction() {

			return new AbstractRedisSlaveAction() {
				
				@Override
				protected void slaveof(List<String> slaveOfCommands) {
					if(logger.isInfoEnabled()){
						logger.info("[slaveof]" + slaveOfCommands);
					}
					changeToSlave(slaveOfCommands);
				}
				
				@Override
				protected String getInfo() {

					Endpoint endpoint = new DefaultEndPoint(redisMasterUri);
					
					String data =
							"run_id:" + keeperRunid + "\r\n" + 
							"role:keeper\r\n" +
							"master_host:" + endpoint.getHost() + "\r\n" +
							"master_port:" + endpoint.getPort() + "\r\n" + 
							"master_link_status:up\r\n" +
							"slave_repl_offset:0\r\n" +
							"slave_priority:0\r\n" + 
							"slave_read_only:" + slaves.size()+
							getSlaveInfo();
						return data;
				}
				
				private String getSlaveInfo(){
					
					StringBuilder slaveInfo = new StringBuilder();
					int index = 0;
					for(Integer port : slaves){
						slaveInfo.append("\r\nslave" + index + ":ip=127.0.0.1,port=" + port + ",state=online,offset=5186,lag=0");
						index++;
						
					}
					return slaveInfo.toString();
				} 
			};
		}
	}

	
	public class SimplePrintSlaveFactory implements IoActionFactory{

		private String runid;
		private String redisMasterUri;
		private String role = "slave";
		private int slavePriority;
		
		public SimplePrintSlaveFactory(String runid, String redisMasterUri, int slavePriority) {
			this.runid = runid;
			this.redisMasterUri = redisMasterUri;
			this.slavePriority = slavePriority;
		}

		public void setRedisMasterUri(String redisMasterUri) {
			this.redisMasterUri = redisMasterUri;
		}
		
		public void setRole(String role){
			this.role = role;
		}
		
		@Override
		public IoAction createIoAction() {
			return new AbstractRedisSlaveAction() {
				
				@Override
				protected String getInfo() {

					Endpoint endpoint = new DefaultEndPoint(redisMasterUri);
					
					String data =
							"run_id:" + runid + "\r\n" + 
							"role:" + role + "\r\n";
					if(role.equalsIgnoreCase("slave")){
							data += "master_host:" + endpoint.getHost() + "\r\n" +
									"master_link_status:up\r\n" +
									"master_port:" + endpoint.getPort() + "\r\n"+
									"slave_repl_offset:0\r\n" +
									"slave_priority:" + slavePriority; 
					}
					return data;
				}
			};
		}
	} 
}
