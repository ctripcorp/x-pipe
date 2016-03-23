package com.ctrip.xpipe.endpoint;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.api.endpoint.Endpoint;

public class DefaultEndPointTest {
	
	
	@Test
	public void testEndPoint(){
		
		String url="redis://:password@10.2.58.242:6379";
		
		Endpoint endpoint = new DefaultEndPoint(url);
		
		
		Assert.assertEquals("redis", endpoint.getScheme());
		Assert.assertEquals("", endpoint.getUser());
		Assert.assertEquals("password", endpoint.getPassword());
		Assert.assertEquals("10.2.58.242", endpoint.getHost());
		Assert.assertEquals(6379, endpoint.getPort());
		
		
		
	}

}
