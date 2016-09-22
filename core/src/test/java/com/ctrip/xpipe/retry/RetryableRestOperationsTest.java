package com.ctrip.xpipe.retry;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Matchers.any;

import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestOperations;

import com.ctrip.xpipe.api.retry.RetryPolicy;
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
		assertEquals("for test", restOperations.getForObject("http://localhost:8080/test", String.class));
		
		try {
			restOperations.put("http://localhost:8080/test", null);
			fail();
		} catch (Exception e) {
			assertTrue(e instanceof HttpClientErrorException);
		} finally {
			ctx.close();
		}
	}

	@Test
	public void retryableRestOperationsFailTest() {
		int retryTimes = 10;
		RetryPolicyFactory mockedRetryPolicyFactory = Mockito.mock(RetryPolicyFactory.class);
		RetryPolicy mockedRetryPolicy = Mockito.mock(RetryPolicy.class);
		when(mockedRetryPolicyFactory.create()).thenReturn(mockedRetryPolicy);
		when(mockedRetryPolicy.retry(any(Throwable.class))).thenReturn(true);
		RestOperations restOperations = RestTemplateFactory.createCommonsHttpRestTemplate(10, 100, 5000, 5000,
				retryTimes, mockedRetryPolicyFactory);
		try {
			restOperations.getForObject("http://localhost:8080/test", String.class);
		} catch (Exception e) {
			verify(mockedRetryPolicy, times(retryTimes)).retry(any(Throwable.class));
			// check the type of original exception
			assertTrue(e instanceof ResourceAccessException);
		}
		
	}

}
