package com.ctrip.xpipe.redis.console.controller.consoleportal.migration;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.sso.UserInfoHolder;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig;
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

/**
 * @author Slight
 * <p>
 * Mar 18, 2022 7:51 PM
 */
public class ExclusiveThreadsForMigrationTest extends AbstractConsoleTest {

    private RestOperations restOperations;
    private SpringApplicationStarter springApplicationStarter;
    private int port = 8080;

    @SpringBootApplication
    @RestController
    @RequestMapping("/api")
    public static class SlowController {

        @GetMapping(value = "/sleep")
        public int syncMigrate() throws InterruptedException {
            TimeUnit.SECONDS.sleep(60);
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
            };
        }

        @Bean
        public ConsoleConfig consoleConfig() {
            return new DefaultConsoleConfig();
        }

        @Bean
        public UserInfoHolder userInfoHolder() {
            return new DefaultUserInfoHolder();
        }
    }

    @Before
    public void beforeRestTemplateFactoryTest() throws Exception {

        restOperations = RestTemplateFactory.createCommonsHttpRestTemplate();

        springApplicationStarter = new SpringApplicationStarter(port, 1, TestDependency.class, SlowController.class, MigrationApi4Beacon.class);
        springApplicationStarter.start();
    }

    @After
    public void afterRestTemplateFactoryTest() throws Exception {
        springApplicationStarter.stop();
    }


    @Test
    public void testMigrationApiWorkProperlyDespiteSlowMethods() throws Exception {

        /* remove implements XPipeMvcRegistrations from TestDependency, this test will fail. */

        BeaconMigrationRequest migrationReq = new BeaconMigrationRequest();
        migrationReq.setClusterName("TEST_CLUSTER");
        migrationReq.setIsForced(true);
        migrationReq.setTargetIDC("OY");

        CountDownLatch latch = new CountDownLatch(1);
        Executors.newSingleThreadExecutor().execute(()->{
            latch.countDown();
            restOperations.getForObject("http://localhost:" + port + "/api/sleep", Integer.class);
        });

        latch.await();
        TimeUnit.MILLISECONDS.sleep(50);
        BeaconMigrationResponse response = restOperations.postForObject("http://localhost:" + port + "/api/beacon/migration/sync", migrationReq, BeaconMigrationResponse.class);
        assertEquals("success", response.getMsg());
    }
}