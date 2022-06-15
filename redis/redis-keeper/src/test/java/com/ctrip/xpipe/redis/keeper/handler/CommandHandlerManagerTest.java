package com.ctrip.xpipe.redis.keeper.handler;

import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.*;

/**
 * @author wenchao.meng
 *
 *         Feb 8, 2017
 */
@RunWith(MockitoJUnitRunner.class)
public class CommandHandlerManagerTest extends AbstractRedisKeeperTest {

	private CommandHandlerManager commandHandlerManager;
	
	private ExecutorService singleExecutors = Executors.newSingleThreadExecutor();

	@Mock
	private RedisClient redisClient;

	@Mock
	private RedisKeeperServer redisKeeperServer;

	@Before
	public void beforeCommandHandlerManagerTest() {
		commandHandlerManager = new CommandHandlerManager();

	}

	@Test
	public void testHandleSequentially() throws Exception {

		List<String> sendCommands = new LinkedList<>();

		doNothing().when(redisClient).sendMessage(argThat(new ArgumentMatcher<ByteBuf>() {

			@Override
			public boolean matches(ByteBuf argument) {
				sendCommands.add(ByteBufUtils.readToString((ByteBuf) argument));
				return true;
			}
		}));

		doNothing().when(redisClient).sendMessage(argThat(new ArgumentMatcher<byte[]>() {

			@Override
			public boolean matches(byte[] argument) {
				sendCommands.add(new String((byte[]) argument));
				return true;
			}
		}));

		doReturn(redisKeeperServer).when(redisClient).getRedisServer();

		doNothing().when(redisKeeperServer).processCommandSequentially(argThat(new ArgumentMatcher<Runnable>() {

			@Override
			public boolean matches(Runnable argument) {
				
				singleExecutors.execute(new Runnable() {

					@Override
					public void run() {
						sleep(1);
						((Runnable) argument).run();
					}
				});
				
				return true;
			}
		}));

		List<String[]> commands = new LinkedList<>();
		commands.add(new String[] { "ping" });
		commands.add(new String[] { "abc" });

		for (int i = 0; i < 5; i++) {


			for (String[] args : commands) {
				commandHandlerManager.handle(args, redisClient);
			}

			sleep(500);
			logger.debug("{}", sendCommands);
			Assert.assertEquals(2, sendCommands.size());
			Assert.assertEquals("+PONG\r\n", sendCommands.get(0));
			
			sendCommands.clear();
		}
	}

}
