package com.ctrip.xpipe.redis.integratedtest;

import java.io.File;

import org.unidal.test.jetty.JettyServer;

/**
 * @author wenchao.meng
 *
 * Jun 24, 2016
 */
public class ConsoleStart extends JettyServer{
	
	private int consolePort = 8080;
	
	public ConsoleStart(int consolePort) {
		this.consolePort = consolePort;
		
	}
	
	public void start() throws Exception{
		
		startServer();
	}

	@Override
	protected String getContextPath() {
		return "/";
	}

	@Override
	protected int getServerPort() {
		return consolePort;
	}

	@Override
	protected File getWarRoot() {
		return new File("../redis-console/src/main/webapp");
	}

	public static void main(String []argc) throws Exception{
		new ConsoleStart(8080).start();
	}
}
