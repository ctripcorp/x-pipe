package com.ctrip.xpipe.redis.console;


import com.ctrip.xpipe.redis.checker.Persistence;
import com.ctrip.xpipe.redis.checker.TestPersistence;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.config.impl.DefaultCheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthChecker;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig;
import com.ctrip.xpipe.redis.console.resources.DefaultPersistence;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = AbstractConsoleIntegrationTest.ConsoleTestConfig.class)
public abstract class AbstractConsoleIntegrationTest extends AbstractConsoleH2DbTest{

    @BeforeClass
    public static void beforeAbstractConsoleIntegrationTest(){
        System.setProperty(HealthChecker.ENABLED, "false");
    }

    @SpringBootApplication
    public static class ConsoleTestConfig{

    }

}
