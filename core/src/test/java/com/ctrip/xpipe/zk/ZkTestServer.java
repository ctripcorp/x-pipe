package com.ctrip.xpipe.zk;

import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import org.apache.curator.test.TestingServer;

/**
 * @author wenchao.meng
 *
 * Jun 25, 2016
 */
public class ZkTestServer extends AbstractLifecycle implements TopElement{
	
	private int zkPort;
	private TestingServer zkServer;

	public ZkTestServer(int zkPort) {
		this.zkPort = zkPort;
	}
	
	@Override
	protected void doInitialize() throws Exception {
		zkServer = new TestingServer(zkPort);
	}
	
	@Override
	protected void doStart() throws Exception {
		zkServer.start();
	}
	
	@Override
	protected void doStop() throws Exception {
		zkServer.stop();
	}
	
	@Override
	protected void doDispose() throws Exception {
		zkServer.close();
	}

	public int getZkPort() {
		return zkPort;
	}
	
	@Override
	public int getOrder() {
		return Ordered.HIGHEST_PRECEDENCE;
	}

}
