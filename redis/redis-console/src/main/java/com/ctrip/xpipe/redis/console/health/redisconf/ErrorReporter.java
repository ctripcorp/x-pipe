package com.ctrip.xpipe.redis.console.health.redisconf;

import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.api.email.EmailService;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.health.HealthChecker;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import com.ctrip.xpipe.redis.console.util.VelocityUtil;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.StringUtil;
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
public class ErrorReporter {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private static final String REDIS_ALERT_TEMPLATE_NAME = "RedisAlertTemplate.vm";

    private Set<RedisConf> versionIssueRedises = new HashSet<>();
    private Set<RedisConf> confIssueRedises = new HashSet<>();

    private boolean redisVersionCollected = false;
    private boolean redisConfCollected = false;

    @Resource(name = ConsoleContextConfig.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduledExecutor;

    @Autowired
    VelocityUtil velocityUtil;

    @Autowired
    private ConsoleConfig consoleConfig;

    @PostConstruct
    public void scheduledTask() {
        int initialDelay = 10, period = 60;
        scheduledExecutor.scheduleAtFixedRate(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                try {
                    if(isRedisConfCollected() && isRedisVersionCollected()) {
                        Email email = Email.DEFAULT;
                        prepareEmail(email);
                        EmailService.DEFAULT.sendEmail(email);
                    }
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
        context.put("versionIssueRedises", versionIssueRedises);
        context.put("confIssueRedises", confIssueRedises);

        email.setBodyContent(velocityUtil.getRenderedString(REDIS_ALERT_TEMPLATE_NAME, context));
        email.setSender(consoleConfig.getRedisAlertSenderEmail());
        fillListWithCommaSeparatedString(email.getRecipients(), consoleConfig.getDBAEmails());
        fillListWithCommaSeparatedString(email.getCCers(), consoleConfig.getRedisAlertCCEmails());
    }

    private void fillListWithCommaSeparatedString(List<String> list, String str) {
        String splitter = "\\s*,\\s*";
        String[] strs = StringUtil.splitRemoveEmpty(splitter, str.trim());
        Collections.addAll(list, strs);
    }

    private void cleanup() {
        redisConfCollected = false;
        redisVersionCollected = false;
        versionIssueRedises.clear();
        confIssueRedises.clear();
    }

    public void addVersionIssueRedis(RedisConf redisConf) {
        versionIssueRedises.add(redisConf);
    }

    public void addConfIssueRedis(RedisConf redisConf) {
        confIssueRedises.add(redisConf);
    }

    private boolean isRedisVersionCollected() {
        return redisVersionCollected;
    }

    public void setRedisVersionCollected(boolean redisVersionCollected) {
        this.redisVersionCollected = redisVersionCollected;
    }

    private boolean isRedisConfCollected() {
        return redisConfCollected;
    }

    public void setRedisConfCollected(boolean redisConfCollected) {
        this.redisConfCollected = redisConfCollected;
    }
}
