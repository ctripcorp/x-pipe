package com.ctrip.xpipe.retry;

import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestOperations;

import com.ctrip.xpipe.simpleserver.SimpleTestSpringServer;
import com.ctrip.xpipe.spring.RestTemplateFactory;

/**
 * @author shyin
 *
 *         Sep 20, 2016
 */
public class RetryableRestOperationsTest {
	@Test
	public void retryableRestOperationsSuccessTest() throws Exception {

		SpringApplication application = new SpringApplication(SimpleTestSpringServer.class);
		ConfigurableApplicationContext ctx = application.run("");
		ctx.start();
		RestOperations restOperations = RestTemplateFactory.createCommonsHttpRestTemplate();
		restOperations.getForObject("http://localhost:8080/test", String.class);
		ctx.close();
	}

	@Test(expected = ResourceAccessException.class)
	public void retryableRestOperationsFailTest() {
		RestOperations restOperations = RestTemplateFactory.createCommonsHttpRestTemplate();
		restOperations.getForObject("http://localhost:8080/test", String.class);
	}
}
