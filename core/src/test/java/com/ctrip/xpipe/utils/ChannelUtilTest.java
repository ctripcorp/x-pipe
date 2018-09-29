package com.ctrip.xpipe.utils;

import com.ctrip.xpipe.AbstractTest;
import io.netty.channel.Channel;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
		
		Assert.assertEquals("L(127.0.0.1:1234)->R(null)", ChannelUtil.getDesc(channel));
	}

}
