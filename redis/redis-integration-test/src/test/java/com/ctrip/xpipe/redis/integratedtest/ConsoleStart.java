package com.ctrip.xpipe.redis.integratedtest;


import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import org.unidal.test.jetty.JettyServer;

import java.io.File;

/**
 * @author wenchao.meng
 *
 * Jun 24, 2016
 */
public class ConsoleStart extends AbstractLifecycle{
	private int consolePort = 8080;
	private ConsoleJettyServer consoleJettyServer;
	
	public ConsoleStart(int consolePort) {
		this.consolePort = consolePort;
		consoleJettyServer = new ConsoleJettyServer();
	}
	@Override
	protected void doStart() throws Exception {
		consoleJettyServer.startServer();
		
	}

	@Override
	protected void doStop() throws Exception {
		consoleJettyServer.stopServer();
	}

	
	private class ConsoleJettyServer extends JettyServer{
		
		@Override
		protected String getContextPath() {
			return "/";
		}

		@Override
		protected int getServerPort() {
			return consolePort;
		}

		@Override
		protected void startServer() throws Exception {
			super.startServer();
		}

		@Override
		protected void stopServer() throws Exception {
			super.stopServer();
		}
		
		@Override
		protected File getWarRoot() {
			return new File("../redis-console/src/main/webapp");
		}
	}


	public static void main(String []argc) throws Exception{
		
		ConsoleStart consoleStart = new ConsoleStart(8080);
		consoleStart.initialize();
		consoleStart.start();
	}
}
