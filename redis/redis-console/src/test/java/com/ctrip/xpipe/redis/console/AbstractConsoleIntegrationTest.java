package com.ctrip.xpipe.redis.console;

import com.ctrip.xpipe.redis.checker.healthcheck.HealthChecker;
import org.apache.velocity.app.VelocityEngine;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Properties;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = AbstractConsoleIntegrationTest.ConsoleTestConfig.class)
public abstract class AbstractConsoleIntegrationTest extends AbstractConsoleDbTest {

    @BeforeClass
    public static void beforeAbstractConsoleIntegrationTest(){
        System.setProperty(HealthChecker.ENABLED, "false");
    }

    @SpringBootApplication
    public static class ConsoleTestConfig{

        @Bean
        public VelocityEngine getVelocityEngine() {
            Properties props = new Properties();
            props.put("resource.loader", "class");
            props.put("class.resource.loader.class", "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
            return new VelocityEngine(props);
        }

    }

}
