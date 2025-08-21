package com.ctrip.xpipe.retry;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.retry.RetryPolicy;
import com.ctrip.xpipe.simpleserver.SimpleTestSpringServer;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.boot.Banner.Mode;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestOperations;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author shyin
 *
 *         Sep 20, 2016
 */
public class RetryableRestOperationsTest extends AbstractTest {
	ConfigurableApplicationContext ctx;
	int port;
	String targetResponse;

	@Before
	public void startUp() {
		port = randomPort(8081, 9090);
		targetResponse = randomString();
		System.setProperty("server.port", String.valueOf(port));
		System.setProperty("target-response", targetResponse);
		SpringApplication app = new SpringApplication(SimpleTestSpringServer.class);
		app.setBannerMode(Mode.OFF);
		ctx = app.run("");
		ctx.start();
	}

	@After
	public void tearDown() {
		if (null != ctx && ctx.isActive()) {
			ctx.close();
		}
	}

	@Test
	public void retryableRestOperationsSuccessTest() throws Exception {
		RestOperations restOperations = RestTemplateFactory.createCommonsHttpRestTemplate();
		assertEquals(targetResponse, restOperations.getForObject(generateRequestURL("/test"), String.class));
	}

	@Test
	public void retryableRestOperationsFailWithMethodNotSupportedTest() {
		RestOperations restOperations = RestTemplateFactory.createCommonsHttpRestTemplate();
		try {
			restOperations.put(generateRequestURL("/test"), null);
			fail();
		} catch (Exception e) {
			assertTrue(e instanceof HttpClientErrorException);
		}
	}
	
	@Test
	public void retryableRestOperationsFailAndRetrySuccessTest() throws InterruptedException {
		ctx.close();
		RestOperations restOperations = RestTemplateFactory.createCommonsHttpRestTemplate(10, 100, 5000, 5000, 30,
				RetryPolicyFactories.newRestOperationsRetryPolicyFactory(100));
		Thread appStartThread = new Thread(new Runnable() {
			@Override
			public void run() {
				logger.info(remarkableMessage("New SpringApplication"));
				SpringApplication app2 = new SpringApplication(SimpleTestSpringServer.class);
				app2.setBannerMode(Mode.OFF);
				ctx = app2.run("");
				ctx.start();
			}
		});
		appStartThread.start();
		String response = restOperations.getForObject(generateRequestURL("/test"), String.class);
		assertEquals(targetResponse, response);
		appStartThread.join();
	}

	@Test
	public void retryableRestOperationsFailTest() {
		ctx.close();

		int retryTimes = 10;
		RetryPolicyFactory mockedRetryPolicyFactory = Mockito.mock(RetryPolicyFactory.class);
		RetryPolicy mockedRetryPolicy = Mockito.mock(RetryPolicy.class);
		when(mockedRetryPolicyFactory.create()).thenReturn(mockedRetryPolicy);
		when(mockedRetryPolicy.retry(any(Throwable.class))).thenReturn(true);
		RestOperations restOperations = RestTemplateFactory.createCommonsHttpRestTemplate(10, 100, 5000, 5000,
				retryTimes, mockedRetryPolicyFactory);
		try {
			restOperations.getForObject(generateRequestURL("/test"), String.class);
		} catch (Exception e) {
			verify(mockedRetryPolicy, times(retryTimes)).retry(any(Throwable.class));
			// check the type of original exception
			assertTrue(e instanceof ResourceAccessException);
		}

	}
	
	@Test
	public void retryableRestOperationFailWithHttpServerErrorExceptionTest() {
		RestOperations restOperations = RestTemplateFactory.createCommonsHttpRestTemplate(10, 100, 5000, 5000, 10,
				RetryPolicyFactories.newRestOperationsRetryPolicyFactory(100));
		
		try {
			restOperations.getForObject(generateRequestURL("/httpservererrorexception"), String.class);
			fail();
		} catch (Exception e) {
			assertTrue(e instanceof HttpServerErrorException);
		}
	}

	private String generateRequestURL(String path) {
		return "http://localhost:" + String.valueOf(port) + path;
	}

}
