package com.ctrip.xpipe.redis.console.schedule;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Created by zhuchen on 2017/8/31.
 */
@SpringBootApplication
@EnableScheduling
public class ScheduledOrganizationServiceTest extends AbstractConsoleIntegrationTest {

    @Autowired
    ScheduledOrganizationService scheduledOrganizationService;

    private void start() throws IOException, SQLException {
        SpringApplication.run(ScheduledOrganizationServiceTest.class);
    }

    @Test
    public void testUpdateOrganizations() throws IOException, SQLException {
        start();
        while(true){

        }
    }
}
