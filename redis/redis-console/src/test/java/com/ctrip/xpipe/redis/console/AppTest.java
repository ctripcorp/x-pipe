package com.ctrip.xpipe.redis.console;

import com.ctrip.xpipe.monitor.CatConfig;
import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderElector;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthChecker;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.IOException;
import java.sql.SQLException;

import static com.ctrip.xpipe.redis.checker.config.impl.CheckConfigBean.KEY_REDIS_CONF_CHECK_INTERVAL;

/**
 * @author lepdou 2016-11-09
 */
@SpringBootApplication
@EnableScheduling
@MapperScan("com.ctrip.xpipe.redis.console.mapper")
@ComponentScan("com.ctrip.xpipe.redis.console.spring")
public class AppTest extends AbstratAppTest {

	@BeforeClass
	public static void beforeAppTestClass() {
		System.setProperty(HealthChecker.ENABLED, "true");

		// mysql重置命令，首次连接本地mysql时使用。非本地环境禁止开启
		// System.setProperty("reset.mysql", "true");
	}

	@Before
	public void startUp() {

		System.setProperty(KEY_REDIS_CONF_CHECK_INTERVAL, "15000");
		System.setProperty(HealthChecker.ENABLED, "true");
		System.setProperty(CatConfig.CAT_ENABLED_KEY, "false");
		System.setProperty(KEY_REDIS_CONF_CHECK_INTERVAL, "30000");

	}


	@Test
	public void startConsole8080() throws IOException, SQLException {

		System.setProperty("server.port", "8080");
		start();

	}

	@Test
	public void startConsole8082() throws IOException, SQLException {

		System.setProperty("console.do.checker", "true");
		System.setProperty("console.do.manager", "false");
		SpringApplication.run(App.class);

	}

	@Test
	public void startConsoleWithHealthCheck8080() throws IOException, SQLException {

		try {
			startZk(2181);
			System.setProperty(AbstractProfile.PROFILE_KEY, AbstractProfile.PROFILE_NAME_TEST);
			System.setProperty("server.port", "8080");
			start();
		}catch (Throwable e){
			logger.error("[]", e);
		}

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
	}


	@After
	public void afterAppTest() throws IOException {
		waitForAnyKeyToExit();
	}


}
