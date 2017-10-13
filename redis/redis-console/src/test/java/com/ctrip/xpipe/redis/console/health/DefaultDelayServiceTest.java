package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.health.delay.DelayService;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author shyin
 *
 * Jan 5, 2017
 */
public class DefaultDelayServiceTest extends AbstractConsoleIntegrationTest {
	@Autowired
	private DelayService delayService;
	
	@Override
	public String prepareDatas() {
		try {
			return prepareDatasFromFile("src/test/resources/ping-service-test.sql");
		} catch (Exception ex) {
			logger.error("Prepare data from file failed",ex);
		}
		return "";
	}
	
	@Test
	@DirtiesContext
	public void testPingService() throws IOException {
		Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new Runnable() {
			
			@Override
			public void run() {
				System.out.println(delayService.getDelay(new HostPort("127.0.0.1", 6379)));
				System.out.println(delayService.getDelay(new HostPort("127.0.0.1", 6389)));
				System.out.println(delayService.getDelay(new HostPort("127.0.0.1", 6369)));
				System.out.println(delayService.getDelay(new HostPort("127.0.0.1", 6359)));
			}
		}, 0, 2, TimeUnit.SECONDS);
		waitForAnyKeyToExit();
	}
}
