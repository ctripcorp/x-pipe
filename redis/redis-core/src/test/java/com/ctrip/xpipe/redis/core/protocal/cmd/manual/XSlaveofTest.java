package com.ctrip.xpipe.redis.core.protocal.cmd.manual;

import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.FixedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.cmd.XSlaveofCommand;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author wenchao.meng
 *
 *         Oct 19, 2016
 */
public class XSlaveofTest extends AbstractCommandTest {

	private String host = "10.2.58.242";
	private int port = 6379;
	FixedObjectPool<NettyClient> clientPool = null;

	@Before
	public void beforeXSlaveofTest() throws Exception {
		
		clientPool = createClientPool(host, port);
	}

	@After
	public void afterXSlaveofTest() throws Exception {
		clientPool.getObject().channel().close();
		clientPool.clear();
	}

	@Test
	public void test() throws Exception {
		
		XSlaveofCommand command = new XSlaveofCommand(clientPool, "10.2.58.242", 6479, scheduled);
		command.execute().get();

	}
}
