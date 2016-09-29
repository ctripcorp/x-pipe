package com.ctrip.xpipe.redis.keeper.impl;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.*;

import java.net.InetSocketAddress;

import org.mockito.runners.MockitoJUnitRunner;

import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;

import io.netty.channel.Channel;
import io.netty.channel.DefaultChannelPromise;

/**
 * @author wenchao.meng
 *
 * Sep 13, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultRedisSlaveTest extends AbstractRedisKeeperTest{
	
	@Mock
	public Channel channel;
	
	@Mock
	public RedisKeeperServer redisKeeperServer;
	
	@Before
	public void beforeDefaultRedisSlaveTest(){
		
		when(channel.closeFuture()).thenReturn(new DefaultChannelPromise(channel));
		when(channel.remoteAddress()).thenReturn(new InetSocketAddress(randomPort()));
	}
	
	
	@SuppressWarnings("resource")
	@Test
	public void testWaitRdbTimeout(){
		
		int waitDumpMilli = 1000;
		
		System.setProperty(DefaultRedisSlave.KEY_RDB_DUMP_MAX_WAIT_MILLI, String.valueOf(waitDumpMilli));
		RedisClient redisClient = new DefaultRedisClient(channel, redisKeeperServer);
		
		DefaultRedisSlave defaultRedisSlave = new DefaultRedisSlave(redisClient);
		defaultRedisSlave.waitForRdbDumping();
		
		sleep(waitDumpMilli + 1000);
		
		Assert.assertTrue(!defaultRedisSlave.isOpen());
		
	}

	@SuppressWarnings("resource")
	@Test
	public void testWaitRdbNormal(){
		
		int waitDumpMilli = 1000;
		
		System.setProperty(DefaultRedisSlave.KEY_RDB_DUMP_MAX_WAIT_MILLI, String.valueOf(waitDumpMilli));
		RedisClient redisClient = new DefaultRedisClient(channel, redisKeeperServer);
		
		DefaultRedisSlave defaultRedisSlave = new DefaultRedisSlave(redisClient);
		defaultRedisSlave.waitForRdbDumping();
		
		sleep(waitDumpMilli/2);
		
		defaultRedisSlave.beginWriteRdb(1000, 2);

		sleep(waitDumpMilli);

		Assert.assertTrue(defaultRedisSlave.isOpen());
		
	}

}
