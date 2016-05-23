package com.ctrip.xpipe.utils;

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

}
