package com.ctrip.xpipe.redis.console.alert;

import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.api.email.EmailService;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.health.HealthChecker;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import com.ctrip.xpipe.redis.console.util.VelocityUtil;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.util.internal.ConcurrentSet;
import org.apache.velocity.VelocityContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Oct 10, 2017
 */

@Component
@ConditionalOnProperty(name = { HealthChecker.ENABLED }, matchIfMissing = true)
public class IssueReporter {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private static final String REDIS_ALERT_TEMPLATE_NAME = "RedisAlertTemplate.vm";
    private static final String EMAIL_SUBJECT = "XPipe 报警";

    private ConcurrentSet<RedisAlert> redisConfAlerts = new ConcurrentSet<>();
    private ConcurrentSet<RedisAlert> redisVersionAlerts = new ConcurrentSet<>();
    private ConcurrentSet<RedisAlert> clientInconsisAlerts = new ConcurrentSet<>();
    private ConcurrentSet<RedisAlert> redisConfNotValidAlerts = new ConcurrentSet<>();
    private ConcurrentSet<RedisAlert> clientInstanceNotOkAlerts = new ConcurrentSet<>();


    @Resource(name = ConsoleContextConfig.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduledExecutor;

    @Autowired
    VelocityUtil velocityUtil;

    @Autowired
    private ConsoleConfig consoleConfig;

    @PostConstruct
    public void scheduledTask() {
        int initialDelay = 10, period = 30;
        scheduledExecutor.scheduleAtFixedRate(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                try {
                    Email email = Email.DEFAULT;
                    prepareEmail(email);
                    EmailService.DEFAULT.sendEmail(email);
                    cleanup();
                } catch (Exception e) {
                    logger.error("[scheduledTask]{}", e);
                }
            }
        }, initialDelay, period, TimeUnit.MINUTES);
    }

    protected void prepareEmail(Email email) {
        VelocityContext context = new VelocityContext();
        context.put("time", DateTimeUtils.currentTimeAsString());
        context.put("environment", consoleConfig.getXpipeRuntimeEnvironmentEnvironment());
        context.put("xpipeAdminEmails", consoleConfig.getXPipeAdminEmails());
        context.put("redisConfAlerts", redisConfAlerts);
        context.put("redisVersionAlerts", redisVersionAlerts);
        context.put("clientInconsisAlerts", clientInconsisAlerts);
        context.put("redisConfNotValidAlerts", redisConfNotValidAlerts);
        context.put("clientInstanceNotOkAlerts", clientInstanceNotOkAlerts);

        email.setBodyContent(velocityUtil.getRenderedString(REDIS_ALERT_TEMPLATE_NAME, context));
        email.setSender(consoleConfig.getRedisAlertSenderEmail());
        email.setSubject(EMAIL_SUBJECT);
        fillListWithCommaSeparatedString(email.getRecipients(), consoleConfig.getDBAEmails());
        fillListWithCommaSeparatedString(email.getCCers(), consoleConfig.getRedisAlertCCEmails());
    }

    private void fillListWithCommaSeparatedString(List<String> list, String str) {
        String splitter = "\\s*,\\s*";
        String[] strs = StringUtil.splitRemoveEmpty(splitter, str.trim());
        Collections.addAll(list, strs);
    }

    private void cleanup() {
        synchronized (IssueReporter.this) {
            redisConfAlerts.clear();
            redisVersionAlerts.clear();
            clientInconsisAlerts.clear();
            redisConfNotValidAlerts.clear();
            clientInstanceNotOkAlerts.clear();
        }
    }

    public void addRedisAlert(RedisAlert redisAlert) {
        ALERT_TYPE alertType = redisAlert.getAlertType();
        switch (alertType) {
            case REDIS_CONF:
                redisConfAlerts.add(redisAlert);
                break;
            case CLIENT_INCONSIS:
                clientInconsisAlerts.add(redisAlert);
                break;
            case REDIS_CONF_NOT_VALID:
                redisConfNotValidAlerts.add(redisAlert);
                break;
            case REDIS_VERSION_NOT_VALID:
                redisVersionAlerts.add(redisAlert);
                break;
            case CLIENT_INSTANCE_NOT_OK:
                clientInstanceNotOkAlerts.add(redisAlert);
                break;
            default:
                break;
        }
    }

}
