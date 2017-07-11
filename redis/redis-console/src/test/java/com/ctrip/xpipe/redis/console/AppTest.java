package com.ctrip.xpipe.redis.console;

import java.io.IOException;
import java.sql.SQLException;

import com.ctrip.xpipe.monitor.CatConfig;
import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderElector;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig;
import org.junit.After;
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
		System.setProperty(CatConfig.CAT_ENABLED_KEY, "false");
	}


	@Test
	public void startConsole8080() throws IOException, SQLException {

		System.setProperty("server.port", "8080");
		System.setProperty(DefaultConsoleConfig.KEY_REDIS_CONF_CHECK_INTERVAL, "30000");
		start();
	}

	@Test
	public void startConsole8081() throws IOException, SQLException {
		System.setProperty("server.port", "8081");
		System.setProperty(KEY_H2_PORT, "9124");
		System.setProperty(ConsoleLeaderElector.KEY_CONSOLE_ID, "2");
		start();
	}

	@Override
	protected String prepareDatas() throws IOException {
		return prepareDatasFromFile("src/test/resources/apptest.sql");
	}

	private void start() throws IOException, SQLException {
		SpringApplication.run(AppTest.class);

		startH2Server();
	}


	@After
	public void afterAppTest() throws IOException {
		waitForAnyKeyToExit();
	}


}
