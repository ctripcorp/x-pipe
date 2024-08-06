package com.ctrip.xpipe.redis.console.controller.consoleportal.migration;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.sso.UserInfoHolder;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.redis.checker.config.impl.CheckConfigBean;
import com.ctrip.xpipe.redis.checker.config.impl.CommonConfigBean;
import com.ctrip.xpipe.redis.checker.config.impl.ConsoleConfigBean;
import com.ctrip.xpipe.redis.checker.config.impl.DataCenterConfigBean;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.impl.CombConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.api.migrate.MigrationApi4Beacon;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationResponse;
import com.ctrip.xpipe.redis.console.service.migration.BeaconMigrationService;
import com.ctrip.xpipe.redis.console.spring.XPipeMvcRegistrations;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.ctrip.xpipe.sso.DefaultUserInfoHolder;
import com.ctrip.xpipe.testutils.SpringApplicationStarter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestOperations;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Slight
 * <p>
 * Mar 18, 2022 7:51 PM
 */
public class ExclusiveThreadsForMigrationTest extends AbstractConsoleTest {

    private RestOperations restOperations;
    private SpringApplicationStarter springApplicationStarter;
    private int port = 8080;

    private static volatile CountDownLatch stopSleep;

    @SpringBootApplication
    @RestController
    @RequestMapping("/api")
    public static class SlowController {

        @GetMapping(value = "/sleep")
        public int syncMigrate() throws InterruptedException {
            if (stopSleep != null) {
                stopSleep.await();
            }
            return 0;
        }

    }

    public static class TestDependency implements XPipeMvcRegistrations {

        @Bean
        public BeaconMigrationService beaconMigrationService() {
            return new BeaconMigrationService() {

                @Override
                public CommandFuture<?> migrate(BeaconMigrationRequest migrationRequest) {
                    CommandFuture<String> future = new DefaultCommandFuture<>();
                    future.setSuccess("OK");
                    return future;
                }

                @Override
                public CommandFuture<?> biMigrate(BeaconMigrationRequest migrationRequest) {
                    CommandFuture<String> future = new DefaultCommandFuture<>();
                    future.setSuccess("OK");
                    return future;
                }
            };
        }

        @Bean
        public ConsoleConfig consoleConfig() {
            return new CombConsoleConfig(new CheckConfigBean(FoundationService.DEFAULT),
                    new ConsoleConfigBean(FoundationService.DEFAULT),
                    new DataCenterConfigBean(),
                    new CommonConfigBean());
        }

        @Bean
        public UserInfoHolder userInfoHolder() {
            return new DefaultUserInfoHolder();
        }
    }

    @Before
    public void beforeRestTemplateFactoryTest() throws Exception {
        this.port = randomPort();
        restOperations = RestTemplateFactory.createCommonsHttpRestTemplate(2, 2, 1200, 60000);

        springApplicationStarter = new SpringApplicationStarter(port, 1, SlowController.class, TestDependency.class, MigrationApi4Beacon.class);
        springApplicationStarter.start();
    }

    @After
    public void afterRestTemplateFactoryTest() throws Exception {
        springApplicationStarter.stop();
    }

    @Test
    public void testEnvStartupTime() throws Exception {
        //do nothing
        assertTrue(true);
    }


    @Test
    public void testMigrationApiWorkProperlyDespiteSlowMethods() throws Exception {

        /* remove implements XPipeMvcRegistrations from TestDependency, this test will fail. */

        BeaconMigrationRequest migrationReq = new BeaconMigrationRequest();
        migrationReq.setClusterName("TEST_CLUSTER");
        migrationReq.setIsForced(true);
        migrationReq.setTargetIDC("OY");

        CountDownLatch threadCreated = new CountDownLatch(1);
        /* the 2 latches below are used to reduce time cost of test */
        CountDownLatch sleepResponseReceived = new CountDownLatch(1);
        stopSleep = new CountDownLatch(1);

        Executors.newSingleThreadExecutor().execute(()->{
            threadCreated.countDown();
            restOperations.getForObject("http://localhost:" + port + "/api/sleep", Integer.class);
            sleepResponseReceived.countDown();
        });

        threadCreated.await();
        TimeUnit.MILLISECONDS.sleep(50);
        BeaconMigrationResponse response = restOperations.postForObject("http://localhost:" + port + "/api/beacon/migration/sync", migrationReq, BeaconMigrationResponse.class);
        assertEquals("success", response.getMsg());
        stopSleep.countDown();
        sleepResponseReceived.await();
    }
}