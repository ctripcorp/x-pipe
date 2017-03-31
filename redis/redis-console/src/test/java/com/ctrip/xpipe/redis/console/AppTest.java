package com.ctrip.xpipe.redis.console;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.ctrip.xpipe.redis.console.health.HealthChecker;
import com.ctrip.xpipe.spring.AbstractProfile;

/**
 * @author lepdou 2016-11-09
 */
@SpringBootApplication
public class AppTest extends AbstractConsoleH2DbTest {

	@Before
	public void startUp() {
		System.setProperty(AbstractProfile.PROFILE_KEY, AbstractProfile.PROFILE_NAME_TEST);
		System.setProperty(HealthChecker.ENABLED, "false");
	}

	@Test
	public void startConsole8080() throws IOException {
		System.setProperty("server.port", "8080");
		start();
	}

	@Override
	protected String prepareDatas() throws IOException {
		return prepareDatasFromFile("src/test/resources/apptest.sql");
	}

	private void start() throws IOException {
		SpringApplication.run(AppTest.class);

		waitForAnyKeyToExit();
	}

}
