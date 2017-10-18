package com.ctrip.xpipe.redis.console.alert.sender;

import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.api.email.EmailService;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.utils.DateTimeUtils;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
@Component(EmailSender.ID)
public class EmailSender extends AbstractSender {

    private Logger logger = LoggerFactory.getLogger(getClass());

    public static final String ID = "com.ctrip.xpipe.console.alert.sender.email.sender";

    public static final String CC_ER = "ccers";

    private static final String SCHEDULED_ALERT_TEMPLATE_NAME = "ScheduledRedisAlertTemplate.vm";

    private static final String IMMEDIATE_ALERT_TEMPLATE_NAME = "ImmediateRedisAlertTemplate.vm";

    private static final String EMAIL_SUBJECT = "[XPipe 报警]";

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private VelocityEngine velocityEngine;


    @Override
    public String getId() {
        return ID;
    }

    @Override
    public boolean send(AlertMessageEntity message) {
        Email email = createEmail(message);
        EmailService.DEFAULT.sendEmail(email);
        return false;
    }

    // Todo: Implement this method
    private Email createEmail(AlertMessageEntity message) {
        Email email = new Email();
        email.setSender(consoleConfig.getRedisAlertSenderEmail());
        email.setSubject(EMAIL_SUBJECT + message.getTitle());
        email.setCharset("UTF-8");

        List<String> ccers = message.getParam(CC_ER);
        if(ccers != null && !ccers.isEmpty()) {
            email.setcCers(ccers);
        }
        email.setRecipients(message.getReceivers());
        email.setBodyContent(message.getContent());
        return email;
    }

    private String renderEmailTemplate(String templateName) {
        VelocityContext context = generateCommonContext();
        return getRenderedString(templateName, context);
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

    public String getRenderedString(String templateName, VelocityContext context) {
        StringWriter stringWriter = new StringWriter();
        String encoding = "UTF-8";
        try {
            velocityEngine.mergeTemplate(templateName, encoding, context, stringWriter);
            return stringWriter.toString();
        } catch (Exception e) {
            logger.error("[getRenderedString] Error with velocity:\n{}", e);
        } finally {
            try {
                stringWriter.close();
            } catch (IOException e) {
                logger.error("[getRenderedString] Closing string writer error:\n {}", e);
            }
        }
        return null;
    }
}
