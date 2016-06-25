package com.ctrip.xpipe.redis.console;

import java.io.IOException;

import org.junit.After;
import org.junit.Test;
import org.unidal.test.jetty.JettyServer;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public class StartConsole extends JettyServer{
	
	
	@Test
	public void startConsole() throws Exception{
		startServer();
		
	}

	@Override
	protected String getContextPath() {
		return "/";
	}

	@Override
	protected int getServerPort() {
		return 8088;
	}

	@After
	public void afterStartMetaServer() throws IOException {
		System.out.println("Press any key to exit...");
		System.in.read();
	}

}
