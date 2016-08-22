package com.ctrip.xpipe.utils;

import java.net.InetSocketAddress;

import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;

/**
 * @author wenchao.meng
 *
 * May 23, 2016
 */
public class IpUtilsTest extends AbstractTest{
	
	@Test
	public void testGetFistNonLocalServerAddress(){
		
		logger.info("{}", IpUtils.getFistNonLocalIpv4ServerAddress());
	}

	@Test
	public void testGetIp(){
		
		InetSocketAddress address = new InetSocketAddress("localhost", 6379);
		logger.info("{}", IpUtils.getIp(address));
		logger.info("{}", address.getAddress().getHostAddress());
	}


}
