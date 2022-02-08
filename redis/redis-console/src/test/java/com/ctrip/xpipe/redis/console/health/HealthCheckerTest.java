package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.redis.console.AbstractConsoleDbTest;
import com.ctrip.xpipe.redis.console.build.ComponentsConfigurator;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthChecker;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.servlet.WebMvcAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;

import java.io.IOException;

/**
 * @author marsqing
 *
 *         Dec 1, 2016 11:45:11 AM
 */
@SpringBootApplication
@ComponentScan(basePackages = {"com.ctrip.xpipe.redis.console"})
public class HealthCheckerTest extends AbstractConsoleDbTest {

	@Before
	public void startUp() {
		System.setProperty(AbstractProfile.PROFILE_KEY, AbstractProfile.PROFILE_NAME_TEST);
		System.setProperty(HealthChecker.ENABLED, "true");
		System.setProperty("spring.main.web-environment", "false");
		System.setProperty(ComponentsConfigurator.KEY_XPIPE_LOCATION, "src/test/resources");
	}


	@Test
	public void start() throws IOException {
		SpringApplication.run(HealthCheckerTest.class);
		waitForAnyKeyToExit();
	}

	@Override
	protected String prepareDatas() throws IOException {
		return prepareDatasFromFile("src/test/resources/apptest.sql");
	}

}
