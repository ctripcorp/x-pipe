package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.DefaultChannelPromise;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.when;

/**
 * @author wenchao.meng
 *
 * Feb 8, 2017
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultRedisClientTest extends AbstractRedisKeeperTest{
	
	private DefaultRedisClient redisClient;
	
	@Mock
	private Channel channel;
	
	@Mock
	private RedisKeeperServer redisKeeperServer; 
	
	@Before
	public void beforeDefaultRedisClientTest(){
		
		when(channel.closeFuture()).thenReturn(new DefaultChannelPromise(channel));
		redisClient = new DefaultRedisClient(channel, redisKeeperServer);
	}
	
	@Test
	public void testReadCommands(){
		
		String []commands = new String[]{"*1\r\n$4\r\nPING\r\n"};
		
		List<String[]> args = readCommands(commands);
		Assert.assertEquals(1, args.size());

		
		commands = new String[]{"*1\r\n$4\r\nPING", "\r\n"};
		args = readCommands(commands);
		Assert.assertEquals(1, args.size());

		
		commands = new String[]{"*1\r\n$4\r\nPING", "\r\n", "*1\r\n$4\r\nPING\r\n"};
		args = readCommands(commands);
		Assert.assertEquals(2, args.size());

	}

	private List<String[]> readCommands(String[] commands) {
		
		List<String[]> args = new LinkedList<>(); 
		for(String command : commands){
			
			String []currentArgs = redisClient.readCommands(Unpooled.wrappedBuffer(command.getBytes()));
			if(currentArgs != null){
				args.add(currentArgs);
			}
		}
		return args;
	}
}
