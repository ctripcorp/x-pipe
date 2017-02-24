package com.ctrip.xpipe.redis.core.protocal.cmd;

import java.net.InetSocketAddress;

import org.junit.Test;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;

/**
 * @author wenchao.meng
 *
 *         Feb 24, 2017
 */
public class DefaultSlaveOfCommandTest extends AbstractRedisTest {

	private String host = "127.0.0.1";
	private int port = 6379;

	@Test
	public void testSuccess() throws Exception {

		executeSlaveOf();
	}

	private void executeSlaveOf() throws Exception {

		Command<String> command = new DefaultSlaveOfCommand(
				getXpipeNettyClientKeyedObjectPool().getKeyPool(new InetSocketAddress(host, port)), scheduled);
		String result = command.execute().get();
		logger.info("{}", result);
	}

	@Test
	public void testMulti() throws Exception {

		for (int i = 0; i < 100; i++) {
			
			logger.info("round:{}", i);
			executeSlaveOf();
		}

	}

}
