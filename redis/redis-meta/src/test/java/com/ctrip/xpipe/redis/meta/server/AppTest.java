package com.ctrip.xpipe.redis.meta.server;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;

import com.ctrip.xpipe.lifecycle.SpringComponentLifecycleManager;
import com.ctrip.xpipe.redis.core.foundation.IdcUtil;
import com.ctrip.xpipe.redis.meta.server.dao.memory.MemoryMetaServerDao;

/**
 * @author shyin
 *
 * Jul 28, 2016
 */

@EnableAutoConfiguration
@Import(com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig.class)
public class AppTest extends AbstractMetaServerContextTest{
	
	private int zkPort = IdcUtil.JQ_ZK_PORT;
	private int serverPort = IdcUtil.JQ_METASERVER_PORT;
	
	public AppTest(){
		
	}
	
	@Before
	public void beforeAppTest(){
		
		System.setProperty(SpringComponentLifecycleManager.SPRING_COMPONENT_START_KEY, "true");
	}

	@Test
	public void start9747() throws Exception {
		
		System.setProperty(MemoryMetaServerDao.MEMORY_META_SERVER_DAO_KEY, "metaserver--jq.xml");
		start();
	}

	@Test
	public void start9748() throws Exception {
		
		this.zkPort = IdcUtil.OY_ZK_PORT;
		this.serverPort = IdcUtil.OY_METASERVER_PORT;

		System.setProperty(MemoryMetaServerDao.MEMORY_META_SERVER_DAO_KEY, "metaserver--oy.xml");
		IdcUtil.setToOY();
		start();
	}
	
	
	public void start(){
		
		System.setProperty("server.port", String.valueOf(serverPort));
		startZk(zkPort);
		SpringApplication.run(AppTest.class, new String[]{});
	}

	
	@Override
	protected boolean isStartZk() {
		return false;
	}
	
	@Override
	protected ApplicationContext createSpringContext() {
		return null;
	}
	
	@After
	public void afterAppTest() throws IOException{
		waitForAnyKeyToExit();
	}
}
