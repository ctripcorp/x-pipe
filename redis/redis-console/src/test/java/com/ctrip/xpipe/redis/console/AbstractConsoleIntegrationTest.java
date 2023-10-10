package com.ctrip.xpipe.redis.console;

import com.ctrip.xpipe.redis.checker.healthcheck.HealthChecker;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = AbstractConsoleIntegrationTest.ConsoleTestConfig.class, webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
public abstract class AbstractConsoleIntegrationTest extends AbstractConsoleDbTest {

    @BeforeClass
    public static void beforeAbstractConsoleIntegrationTest(){
        System.setProperty(HealthChecker.ENABLED, "false");
    }

    @SpringBootApplication
    public static class ConsoleTestConfig{

    }

}
