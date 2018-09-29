package com.ctrip.xpipe.redis.core.protocal.cmd.transaction;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.SlaveOfCommand;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

/**
 * manual test with redis
 * @author wenchao.meng
 *
 *         Dec 8, 2016
 */
public class TransactionalCommandTest extends AbstractRedisTest {

	private String ip = "localhost";

	private int port = 6379;

	private int testCount = 10;

	@Test
	public void testTransaction() throws Exception {

		for (int i = 0; i < testCount; i++) {
			TransactionalCommand command = createRightTransaction();
			Object[] result = command.execute().get();
			for (Object o : result) {
				logger.info("{}", o);
			}
		}
	}

	private TransactionalCommand createRightTransaction() throws Exception {

		return new TransactionalCommand(
				getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(ip, port)), scheduled,
				new SlaveOfCommand(null, scheduled), new RoleCommand(null, scheduled));
	}

	@Test
	public void testWrongCommand() throws Exception {

		for(int i=0; i < testCount ; i++){
			try{
				TransactionalCommand command = createWrongTransaction();
				command.execute().get();
				Assert.fail();
			}catch(Exception e){
			}
			
			TransactionalCommand command = createRightTransaction();
			Object[] result = command.execute().get();
			for (Object o : result) {
				logger.info("{}", o);
			}
		}

	}

	private TransactionalCommand createWrongTransaction() throws Exception {

		return new TransactionalCommand(
				getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(ip, port)), scheduled,
				new SlaveOfCommand(null, scheduled), new WrongCommand(null));
	}

	@Test
	public void testArray() {

		Object[] multi = new Object[3];
		multi[0] = "111111111111";
		multi[1] = 22222222;
		multi[2] = "heel";

		logger.info("{}", (Object) multi);
	}

	public static class WrongCommand extends AbstractRedisCommand<String> {

		public WrongCommand(SimpleObjectPool<NettyClient> clientPool) {
			super(clientPool, null);
		}

		@Override
		protected String format(Object payload) {
			return payloadToString(payload);
		}

		@Override
		public ByteBuf getRequest() {
			return Unpooled.wrappedBuffer("wrongcommand\r\n".getBytes());
		}
	}
}
