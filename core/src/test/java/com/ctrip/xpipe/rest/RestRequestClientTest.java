package com.ctrip.xpipe.rest;


import java.util.concurrent.TimeUnit;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.core.Application;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;

/**
 * @author wenchao.meng
 *
 * Jun 29, 2016
 */
public class RestRequestClientTest extends JerseyTest{
	
	private String address;
	private static String restStr = AbstractTest.randomString();
	
	public RestRequestClientTest() {
		System.setProperty(TestProperties.CONTAINER_PORT, String.valueOf(AbstractTest.randomPort()));
	}
	
	@Before
	public void before(){
		address = getBaseUri().toString();
	}
	
	@Test
	public void testGet(){
		
		Assert.assertEquals(restStr, RestRequestClient.get(address + "hello", String.class));
	}

	@Test(expected = ProcessingException.class)
	public void testGetTimeout(){
		
		RestRequestClient.get(address+"timeout6", String.class, 1000, 1000);
	}
	
	@Override
	protected Application configure() {
		return new ResourceConfig(TestJersey.class);
	}

	@Path("/")
	public static class TestJersey{
		
		@Path("/hello")
		@GET
		public String get(){
			return restStr;
		}
		
		@Path("/timeout6")
		@GET
		public String timeout() throws InterruptedException{
			TimeUnit.SECONDS.sleep(6);
			return restStr;
		}
	}

}
