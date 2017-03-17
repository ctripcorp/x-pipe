package com.ctrip.xpipe.redis.console.health;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.EmbeddedServletContainerAutoConfiguration;
import org.springframework.boot.autoconfigure.web.WebMvcAutoConfiguration;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.build.ComponentsConfigurator;
import com.ctrip.xpipe.spring.AbstractProfile;

/**
 * @author marsqing
 *
 *         Dec 1, 2016 11:45:11 AM
 */
@SpringBootApplication(exclude = { EmbeddedServletContainerAutoConfiguration.class, WebMvcAutoConfiguration.class })
public class HealthCheckerTest extends AbstractConsoleTest {

	@Before
	public void startUp() {
		System.setProperty(AbstractProfile.PROFILE_KEY, AbstractProfile.PROFILE_NAME_TEST);
		System.setProperty("spring.main.web-environment", "false");
		System.setProperty(ComponentsConfigurator.KEY_XPIPE_LOCATION, "src/test/resources");
	}

	@Test
	public void start() throws IOException {
		SpringApplication.run(HealthCheckerTest.class);
		waitForAnyKeyToExit();
	}

}
