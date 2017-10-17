package com.ctrip.xpipe.redis.console.alert;

import com.ctrip.xpipe.api.email.EMAIL_TYPE;
import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;


/**
 * @author chen.zhu
 * <p>
 * Oct 16, 2017
 */
public class EmailReporterTest extends AbstractConsoleIntegrationTest {

    @Autowired
    ConsoleConfig consoleConfig;

    @Autowired
    EmailReporter emailReporter;

    @Test
    public void prepareScheduledEmail() throws Exception {
        Map<ALERT_TYPE, Set<RedisAlert>> map = new HashMap<>();
        map.put(ALERT_TYPE.CLIENT_INCONSIS, generateRedisAlertSet(5, ALERT_TYPE.CLIENT_INCONSIS));
        map.put(ALERT_TYPE.MIGRATION_MANY_UNFINISHED, generateRedisAlertSet(5, ALERT_TYPE.MIGRATION_MANY_UNFINISHED));
        map.put(ALERT_TYPE.REDIS_REPL_DISKLESS_SYNC_ERROR, generateRedisAlertSet(5, ALERT_TYPE.REDIS_REPL_DISKLESS_SYNC_ERROR));
        Collection<Email> emails = emailReporter.prepareScheduledEmail(map);
        emails.forEach(email -> {
            logger.info("==================I'm Splitter====================");
            logger.info("Email Type: {}", email.getEmailType());
            logger.info("Receivers: {}", email.getRecipients());
            logger.info("CCers: {}", email.getCCers());
            logger.info("email body: \n{}", email.getBodyContent());
            logger.info("==================I'm Splitter====================");
        });
    }

    @Test
    public void getEmailTypeSeparatedRedisAlerts() throws Exception {
        Map<ALERT_TYPE, Set<RedisAlert>> map = new HashMap<>();
        map.put(ALERT_TYPE.CLIENT_INCONSIS, generateRedisAlertSet(5, ALERT_TYPE.CLIENT_INCONSIS));
        map.put(ALERT_TYPE.MIGRATION_MANY_UNFINISHED, generateRedisAlertSet(5, ALERT_TYPE.MIGRATION_MANY_UNFINISHED));
        map.put(ALERT_TYPE.REDIS_REPL_DISKLESS_SYNC_ERROR, generateRedisAlertSet(5, ALERT_TYPE.REDIS_REPL_DISKLESS_SYNC_ERROR));
        Map<ALERT_TYPE, Set<RedisAlert>> result = emailReporter
                .getEmailTypeSeparatedRedisAlerts(map, EMAIL_TYPE.SEND_TO_DBA_CC_DEV);
        Assert.assertNotEquals(map, result);
        Set<ALERT_TYPE> expectedSet = new HashSet<>();
        expectedSet.addAll(
                Arrays.asList(new ALERT_TYPE[]{ALERT_TYPE.CLIENT_INCONSIS, ALERT_TYPE.REDIS_REPL_DISKLESS_SYNC_ERROR}));
        Assert.assertEquals(expectedSet, result.keySet());
    }

    @Test
    public void prepareImmediateEmail() throws Exception {
        Email email = emailReporter.prepareImmediateEmail(new RedisAlert(new HostPort("192.168.1.10", 6379),
                "cluster", "shard", "new in come", ALERT_TYPE.XREDIS_VERSION_NOT_VALID));
        logger.info("==================I'm Splitter====================");
        logger.info("Email Type: {}", email.getEmailType());
        logger.info("Receivers: {}", email.getRecipients());
        logger.info("CCers: {}", email.getCCers());
        logger.info("email body: \n{}", email.getBodyContent());
        logger.info("==================I'm Splitter====================");
    }

    @Test
    public void scheduledReport() {
        Map<ALERT_TYPE, Set<RedisAlert>> map = new HashMap<>();
        map.put(ALERT_TYPE.CLIENT_INCONSIS, generateRedisAlertSet(5, ALERT_TYPE.CLIENT_INCONSIS));
        map.put(ALERT_TYPE.MIGRATION_MANY_UNFINISHED, generateRedisAlertSet(5, ALERT_TYPE.MIGRATION_MANY_UNFINISHED));
        map.put(ALERT_TYPE.REDIS_REPL_DISKLESS_SYNC_ERROR, generateRedisAlertSet(5, ALERT_TYPE.REDIS_REPL_DISKLESS_SYNC_ERROR));
        map.put(ALERT_TYPE.XREDIS_VERSION_NOT_VALID, generateRedisAlertSet(3, ALERT_TYPE.XREDIS_VERSION_NOT_VALID));
        emailReporter.scheduledReport(map);
    }

    @Test
    public void immediateReport() {
        RedisAlert redisAlert = new RedisAlert(new HostPort("192.168.1.10", 6379),
                "cluster", "shard", "", ALERT_TYPE.XREDIS_VERSION_NOT_VALID);
        emailReporter.immediateReport(redisAlert);
    }

    private Set<RedisAlert> generateRedisAlertSet(int count, ALERT_TYPE alertType) {
        int index = 0;
        Set<RedisAlert> result = new HashSet<>();
        while(index++ < count) {
            RedisAlert redisAlert = new RedisAlert(new HostPort("192.168.1.10", index),
                    "cluster"+index, "shard"+index, "", alertType);
            result.add(redisAlert);
        }
        return result;
    }
}