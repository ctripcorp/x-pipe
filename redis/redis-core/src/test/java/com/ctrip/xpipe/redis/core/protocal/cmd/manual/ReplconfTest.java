package com.ctrip.xpipe.redis.core.protocal.cmd.manual;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.FixedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.GapAllowedSync;
import com.ctrip.xpipe.redis.core.protocal.cmd.InMemoryGapAllowedSync;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf.ReplConfType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author wenchao.meng
 *
 *         Oct 19, 2016
 */
public class ReplconfTest extends AbstractCommandTest {

	private String host = "127.0.0.1";
	
	private int port = 6379;

	@Before
	public void beforeReplconfTest() throws Exception {

	}
	
	@Test
	public void testCapa() throws Exception{
		
		Replconf conf = new Replconf(getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(host, port)), ReplConfType.CAPA, scheduled, "eof", "psync2");
		logger.info("{}", conf.execute().get());		
	}

	@Test
	public void test() throws Exception {

		for (int i = 0; i < 100; i++) {

			FixedObjectPool<NettyClient> clientPool = null;

			try {
				clientPool = createClientPool(host, port);

				Replconf replconf = new Replconf(clientPool, ReplConfType.LISTENING_PORT, scheduled, String.valueOf(1234));
				replconf.execute().addListener(new CommandFutureListener<Object>() {

					@Override
					public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
						logger.info("{}", commandFuture.get());
					}
				});

				GapAllowedSync gasync = new InMemoryGapAllowedSync(clientPool, "?", -1L, scheduled);
				try {
					gasync.execute().get(100, TimeUnit.MILLISECONDS);
					Assert.fail();
				} catch (TimeoutException e) {
				}
			} finally {
				if (clientPool != null) {
					clientPool.getObject().channel().close();
				}
			}
		}
	}
}
