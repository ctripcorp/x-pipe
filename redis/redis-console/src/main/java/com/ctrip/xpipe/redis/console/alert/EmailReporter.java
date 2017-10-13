package com.ctrip.xpipe.redis.console.alert;

import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.api.email.EmailService;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.util.VelocityUtil;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.StringUtil;
import org.apache.velocity.VelocityContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 13, 2017
 */
@Component
public class EmailReporter implements Reporter {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    VelocityUtil velocityUtil;

    private static final String REDIS_ALERT_TEMPLATE_NAME = "RedisAlertTemplate.vm";

    private static final String EMAIL_SUBJECT = "XPipe 报警";

    @Override
    public void report(RedisAlert redisAlert) {
        Email email = new Email();
        prepareEmail(email);
        EmailService.DEFAULT.sendEmail(email);
    }

    @Override
    public void report(Collection<RedisAlert> redisAlerts) {
        Email email = new Email();
        prepareEmail(email);
        EmailService.DEFAULT.sendEmail(email);
    }

    protected void prepareEmail(Email email) {
        VelocityContext context = new VelocityContext();
        context.put("time", DateTimeUtils.currentTimeAsString());
        context.put("environment", consoleConfig.getXpipeRuntimeEnvironmentEnvironment());
        context.put("xpipeAdminEmails", consoleConfig.getXPipeAdminEmails());

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
}
