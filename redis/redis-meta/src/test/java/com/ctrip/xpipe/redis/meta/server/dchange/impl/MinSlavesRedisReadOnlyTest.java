package com.ctrip.xpipe.redis.meta.server.dchange.impl;

import com.ctrip.xpipe.redis.core.protocal.cmd.ConfigGetCommand.ConfigGetMinSlavesToWrite;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.MinSlavesRedisReadOnly;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;

/**
 * 
 * @author wenchao.meng
 *
 * Jan 22, 2017
 */
@SuppressWarnings("deprecation")
public class MinSlavesRedisReadOnlyTest extends AbstractMetaServerTest{
	
	private MinSlavesRedisReadOnly minSlavesRedisReadOnly;
	private String host = "localhost";
	private int port = 6379;
	
	
	@Before
	public void beforeMinSlavesRedisReadOnlyTest() throws Exception{
		
		minSlavesRedisReadOnly = new MinSlavesRedisReadOnly(host, port, getXpipeNettyClientKeyedObjectPool(), scheduled);
	}
	
	@Test //manual start redis at host:port
	public void testMark() throws Exception{
		
		minSlavesRedisReadOnly.makeReadOnly();
		int number = new ConfigGetMinSlavesToWrite(getXpipeNettyClientKeyedObjectPool().getKeyPool(new InetSocketAddress(host, port)), scheduled).execute().get();
		Assert.assertEquals(MinSlavesRedisReadOnly.READ_ONLY_NUMBER, number);
		
		minSlavesRedisReadOnly.makeWritable();
		number = new ConfigGetMinSlavesToWrite(getXpipeNettyClientKeyedObjectPool().getKeyPool(new InetSocketAddress(host, port)), scheduled).execute().get();
		Assert.assertEquals(MinSlavesRedisReadOnly.WRITABLE_NUMBER, number);
	}

}
