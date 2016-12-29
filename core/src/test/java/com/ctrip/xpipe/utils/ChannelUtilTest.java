package com.ctrip.xpipe.utils;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;

import io.netty.channel.Channel;

import static org.mockito.Mockito.*;

import java.net.InetSocketAddress;

/**
 * @author wenchao.meng
 *
 * Dec 27, 2016
 */
public class ChannelUtilTest extends AbstractTest{
	
	@Test
	public void test(){
		
		Channel channel = mock(Channel.class);
		Assert.assertEquals("L(null)->R(null)", ChannelUtil.getDesc(channel));

		when(channel.localAddress()).thenReturn(new InetSocketAddress("localhost", 1234));
		
		Assert.assertEquals("L(localhost:1234)->R(null)", ChannelUtil.getDesc(channel));
	}

}
