package com.ctrip.xpipe.redis.console.alert;

import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.health.HealthChecker;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author chen.zhu
 * <p>
 * Oct 13, 2017
 */
public class IssueReporterTest extends AbstractConsoleIntegrationTest {

    //@Autowired
    //IssueReporter reporter;

    @BeforeClass
    public static void beforeIssueReporterTestClass() {
        System.setProperty(HealthChecker.ENABLED, "true");
    }

    @Test
    public void prepareEmail() throws Exception {
//        HostPort hostPort = new HostPort("10.3.2.23", 6379);
//        reporter.addRedisAlert(new RedisAlert(hostPort, "cluster1", "shard1", "nothing", ALERT_TYPE.CLIENT_INCONSIS));
//        reporter.addRedisAlert(new RedisAlert(hostPort, "cluster1", "shard1", "nothing", ALERT_TYPE.CLIENT_INSTANCE_NOT_OK));
//        reporter.addRedisAlert(new RedisAlert(hostPort, "cluster1", "shard1", "nothing", ALERT_TYPE.REDIS_CONF_REWRITE_UNEXECUTABLE));
//        reporter.addRedisAlert(new RedisAlert(hostPort, "cluster1", "shard1", "nothing", ALERT_TYPE.REDIS_REPL_DISKLESS_SYNC_ERROR));
//        reporter.addRedisAlert(new RedisAlert(hostPort, "cluster1", "shard1", "nothing", ALERT_TYPE.XREDIS_VERSION_NOT_VALID));
//        reporter.addRedisAlert(new RedisAlert(hostPort, "cluster2", "shard2", "nothing", ALERT_TYPE.CLIENT_INCONSIS));
//
//        Email email = Email.DEFAULT;
//        reporter.prepareEmail(email);
//        logger.info("HTML Body:\n{}", email.getBodyContent());
    }

}