package com.ctrip.xpipe.redis.console.alert;

import com.ctrip.xpipe.api.email.EMAIL_TYPE;
import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.api.email.EmailService;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.util.EmailUtil;
import com.ctrip.xpipe.redis.console.util.VelocityUtil;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.StringUtil;
import org.apache.velocity.VelocityContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author chen.zhu
 * <p>
 * Oct 13, 2017
 */
@Component
public class EmailReporter implements Reporter {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    ConsoleConfig consoleConfig;

    @Autowired
    VelocityUtil velocityUtil;

    @Autowired
    EmailUtil emailUtil;

    private static final String SCHEDULED_ALERT_TEMPLATE_NAME = "ScheduledRedisAlertTemplate.vm";

    private static final String IMMEDIATE_ALERT_TEMPLATE_NAME = "ImmediateRedisAlertTemplate.vm";

    private static final String EMAIL_SUBJECT = "XPipe 报警";

    @Override
    public void immediateReport(RedisAlert redisAlert) {
        Email email = prepareImmediateEmail(redisAlert);
        EmailService.DEFAULT.sendEmail(email);
    }

    @Override
    public void scheduledReport(Map<ALERT_TYPE, Set<RedisAlert>> redisAlerts) {
        Collection<Email> emails = prepareScheduledEmail(redisAlerts);
        emails.forEach(email -> EmailService.DEFAULT.sendEmail(email));
    }

    protected Collection<Email> prepareScheduledEmail(Map<ALERT_TYPE, Set<RedisAlert>> redisAlerts) {
        EMAIL_TYPE[] emailTypes = EMAIL_TYPE.values();
        List<Email> emails = new LinkedList<>();
        for(EMAIL_TYPE emailType : emailTypes) {
            Map<ALERT_TYPE, Set<RedisAlert>> redisAlertMap = getEmailTypeSeparatedRedisAlerts(redisAlerts, emailType);
            if(redisAlertMap.isEmpty()) {
                logger.info("[prepareScheduledEmail] Email Type: {}, set is empty", emailType);
                continue;
            }
            Email email = createEmail();
            email.setEmailType(emailType);
            emailUtil.fillRecipientsAndCCersByType(email);

            VelocityContext context = generateCommonContext();
            context.put("redisAlerts", redisAlertMap);
            email.setBodyContent(velocityUtil.getRenderedString(SCHEDULED_ALERT_TEMPLATE_NAME, context));

            emails.add(email);
        }
        return emails;
    }

    protected Map<ALERT_TYPE, Set<RedisAlert>> getEmailTypeSeparatedRedisAlerts(
            Map<ALERT_TYPE, Set<RedisAlert>> redisAlerts, EMAIL_TYPE emailType) {

        Map<ALERT_TYPE, Set<RedisAlert>> result = new HashMap<>(redisAlerts);
        Set<ALERT_TYPE> filterSet = result.keySet().stream()
                .filter(alertType -> alertType.emailType() == emailType).collect(Collectors.toSet());
        result.keySet().retainAll(filterSet);
        return result;
    }

    protected Email prepareImmediateEmail(RedisAlert redisAlert) {
        VelocityContext context = generateCommonContext();
        context.put("redisAlert", redisAlert);

        Email email = createEmail();
        email.setEmailType(redisAlert.getAlertType().emailType());
        emailUtil.fillRecipientsAndCCersByType(email);
        email.setBodyContent(velocityUtil.getRenderedString(IMMEDIATE_ALERT_TEMPLATE_NAME, context));
        return email;
    }

    private VelocityContext generateCommonContext() {
        VelocityContext context = new VelocityContext();
        context.put("time", DateTimeUtils.currentTimeAsString());
        context.put("environment", consoleConfig.getXpipeRuntimeEnvironmentEnvironment());
        context.put("xpipeAdminEmails", consoleConfig.getXPipeAdminEmails());

        context.put("XREDIS_VERSION_NOT_VALID", ALERT_TYPE.XREDIS_VERSION_NOT_VALID);
        context.put("CLIENT_INCONSIS", ALERT_TYPE.CLIENT_INCONSIS);
        context.put("MIGRATION_MANY_UNFINISHED", ALERT_TYPE.MIGRATION_MANY_UNFINISHED);
        context.put("REDIS_REPL_DISKLESS_SYNC_ERROR", ALERT_TYPE.REDIS_REPL_DISKLESS_SYNC_ERROR);
        context.put("CLIENT_INSTANCE_NOT_OK", ALERT_TYPE.CLIENT_INSTANCE_NOT_OK);
        context.put("MIGRATION_MANY_UNFINISHED", ALERT_TYPE.MIGRATION_MANY_UNFINISHED);
        context.put("QUORUM_DOWN_FAIL", ALERT_TYPE.QUORUM_DOWN_FAIL);
        context.put("SENTINEL_RESET", ALERT_TYPE.SENTINEL_RESET);

        return context;
    }

    private Email createEmail() {
        Email email = new Email();
        email.setSender(consoleConfig.getRedisAlertSenderEmail());
        email.setSubject(EMAIL_SUBJECT);
        email.setCharset("UTF-8");
        return email;
    }

}
