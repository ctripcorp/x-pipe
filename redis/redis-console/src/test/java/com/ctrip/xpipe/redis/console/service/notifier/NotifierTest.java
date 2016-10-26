package com.ctrip.xpipe.redis.console.service.notifier;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.console.server.TestConsoleServer;

/**
 * @author shyin
 *
 * Oct 28, 2016
 */
public class NotifierTest extends AbstractTest {
	private ConfigurableApplicationContext testMetaserver;
	private ConfigurableApplicationContext testConsole;
	private int port = 8080;
	private int port2 = 8081;
	
	@Before
	public void setUp() {
		System.setProperty("server.port", String.valueOf(port));
		SpringApplication app = new SpringApplication(TestConsoleServer.class);
		testMetaserver = app.run("");
		testMetaserver.start();
		System.setProperty("server.port", String.valueOf(port2));
		SpringApplication app2 = new SpringApplication(TestConsoleServer.class);
		testConsole = app2.run("");
		testConsole.start();
	}
	
	@After
	public void tearDown() {
		if( null != testMetaserver && testMetaserver.isActive()) {
			testMetaserver.close();
		}
		if(null != testConsole && testConsole.isActive()) {
			testConsole.close();
		}
	}
	
	@Test
	public void testNotifySuccess() throws IOException {
		testConsole.getBean(ClusterMetaModifiedNotifier.class).notifyClusterUpdate("jq", "cluster");
		waitForAnyKeyToExit();
	}
	
	@Test
	public void testNotifyFailed() throws IOException {
		testMetaserver.close();
		testConsole.getBean(ClusterMetaModifiedNotifier.class).notifyClusterUpdate("jq", "cluster");
		waitForAnyKeyToExit();
	}
	
}
