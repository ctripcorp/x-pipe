package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.redis.checker.alert.AlertConfig;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.config.impl.DefaultCheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthChecker;
import com.ctrip.xpipe.redis.checker.impl.TestMetaCache;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author lishanglin
 * date 2021/3/9
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = AbstractCheckerIntegrationTest.CheckerTestConfig.class)
public class AbstractCheckerIntegrationTest extends AbstractCheckerTest {

    protected String[] dcNames = new String[]{"jq", "oy"};

    @BeforeClass
    public static void beforeAbstractCheckerIntegrationTest(){
        System.setProperty(HealthChecker.ENABLED, "false");

    }

    @SpringBootApplication
    public static class CheckerTestConfig{

        @Bean
        public MetaCache metaCache() {
            return new TestMetaCache();
        }

        @Bean
        public TestPersistence persistence() {
            return new TestPersistence();
        }

        @Bean
        public TestConfig testConfig() {
            return new TestConfig();
        }

        @Bean
        public CheckerConfig checkerConfig(TestConfig config) {
            return config;
        }

        @Bean
        public AlertConfig alertConfig(TestConfig config) {
            return config;
        }

        @Bean
        public CheckerDbConfig checkerDbConfig(Persistence persistence) {
            return new DefaultCheckerDbConfig(persistence);
        }

    }

}
