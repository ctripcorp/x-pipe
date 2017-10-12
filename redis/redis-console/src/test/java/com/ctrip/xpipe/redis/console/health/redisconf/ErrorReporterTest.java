package com.ctrip.xpipe.redis.console.health.redisconf;

import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.health.HealthChecker;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 12, 2017
 */
public class ErrorReporterTest extends AbstractConsoleIntegrationTest {

    @Autowired
    ErrorReporter errorReporter;

    @BeforeClass
    public static void beforeErrorReporterTestClass() {
        System.setProperty(HealthChecker.ENABLED, "true");
    }

    @Before
    public void beforeErrorReporterTest() {
        errorReporter.addConfIssueRedis(new RedisConf(new HostPort("10.3.2.23", 6379), "confIssueCluster", "confIssueShard"));
        errorReporter.addVersionIssueRedis(new RedisConf(new HostPort("10.3.2.23", 6379), "versionIssueCluster", "versionIssueShard"));
    }

    @Test
    public void prepareEmail() throws Exception {
        Email email = Email.DEFAULT;
        errorReporter.prepareEmail(email);
        logger.info("Email Body: \n{}", email.getBodyContent());
    }

}