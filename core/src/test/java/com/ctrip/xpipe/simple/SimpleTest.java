package com.ctrip.xpipe.simple;

import org.junit.Test;

import com.dianping.cat.configuration.client.entity.ClientConfig;

/**
 * @author wenchao.meng
 *
 * Aug 24, 2016
 */
public class SimpleTest {
	
	@Test
	public <V> void testCommand(){
		ClientConfig clientConfig = new ClientConfig();
		System.out.println(clientConfig);
	}

}
