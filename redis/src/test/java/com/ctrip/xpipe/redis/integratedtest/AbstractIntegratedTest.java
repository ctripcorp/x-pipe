package com.ctrip.xpipe.redis.integratedtest;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.junit.After;
import org.junit.Before;

import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.metaserver.StartMetaServer;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author wenchao.meng
 *
 * Jun 13, 2016
 */
public class AbstractIntegratedTest extends AbstractRedisKeeperTest{

	private StartMetaServer startMetaServer;
	private int metaServerPort = 9747;
	
	private int redisMasterPort = 6379;
	private int keeperPort1 = 7777;
	private int keeperPort2 = 7778;
	private int keeperSlave1Port = 8888;
	private int keeperSlave2Port = 8889;
	
	
	
	@Before
	public void beforeAbstractIntegratedTest() throws Exception{
		
		stopAllRedisServer();
		stopMetaServer();
		startMetaServer();
	}

	protected void stopAllRedisServer() throws ExecuteException, IOException {
		
		executeScript("scripts/kill_redis.sh");
	}

	protected void startRedisMaster(){
		
		
		
	}
	
	protected void startMetaServer() throws Exception {
		
		logger.info("[startMetaServer]");
		startMetaServer = new StartMetaServer();
		startMetaServer.setServerPort(metaServerPort);
		startMetaServer.start();
	}


	protected void stopMetaServer() throws ExecuteException, IOException {
		
		logger.info("[stopMetaServer]");
		executeScript("scripts/kill_meta_server.sh", String.valueOf(metaServerPort));
	}

	protected void executeScript(String file, String ...args) throws ExecuteException, IOException{
		
		 URL url = getClass().getClassLoader().getResource(file);
		 if(url == null){
			 throw new FileNotFoundException(file);
		 }
		DefaultExecutor executor = new DefaultExecutor();
		executor.execute(CommandLine.parse("sh -v " + url.getFile() + " " + StringUtil.join(" ", args)));
	}
	
	@After
	public void afterAbstractIntegratedTest(){
		
	}
}
