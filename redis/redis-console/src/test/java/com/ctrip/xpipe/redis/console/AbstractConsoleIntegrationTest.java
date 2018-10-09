package com.ctrip.xpipe.redis.console;


import com.ctrip.xpipe.redis.console.healthcheck.HealthChecker;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.SpringApplicationConfiguration;
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
