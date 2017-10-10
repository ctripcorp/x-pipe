package com.ctrip.xpipe.redis.console.health.redisconf;

import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.api.email.EmailService;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.health.HealthChecker;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
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

    private Set<RedisConf> versionIssueRedises = new HashSet<>();
    private Set<RedisConf> confIssueRedises = new HashSet<>();

    private boolean redisVersionCollected = false;
    private boolean redisConfCollected = false;

    @Resource(name = ConsoleContextConfig.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduledExecutor;

    @PostConstruct
    public void scheduledTask() {
        scheduledExecutor.scheduleAtFixedRate(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                try {
                    if(redisConfCollected && redisVersionCollected) {
                        Email email = Email.DEFAULT;
                        List[] context = buildEmailContext();
                        EmailService.DEFAULT.sendEmail(email, context);
                    }
                    redisConfCollected = false;
                    redisVersionCollected = false;
                    versionIssueRedises.clear();
                    confIssueRedises.clear();
                } catch (Exception e) {
                    logger.error("[scheduledTask]{}", e);
                }
            }
        }, 10, 30, TimeUnit.MINUTES);
    }

    private List[] buildEmailContext() {
        List<String> versionErrorRedises = convertRedisConfSetToStringList(versionIssueRedises);
        List<String> confErrorRedises = convertRedisConfSetToStringList(confIssueRedises);

        return new List[]{versionErrorRedises, confErrorRedises};
    }

    private List<String> convertRedisConfSetToStringList(Set<RedisConf> redisConfSet) {
        List<String> result = new LinkedList<>();
        for(RedisConf redisConf : redisConfSet) {
            String info = redisConf.getClusterId() + ","
                    + redisConf.getShardId() + ","
                    + redisConf.getHostPort().getHost() + ","
                    + redisConf.getHostPort().getPort();
            result.add(info);
        }
        return result;
    }

    public void addVersionIssueRedis(RedisConf redisConf) {
        versionIssueRedises.add(redisConf);
    }

    public void addConfIssueRedis(RedisConf redisConf) {
        confIssueRedises.add(redisConf);
    }

    public Set<RedisConf> getVersionIssueRedises() {
        return versionIssueRedises;
    }

    public void setVersionIssueRedises(Set<RedisConf> versionIssueRedises) {
        this.versionIssueRedises = versionIssueRedises;
    }

    public Set<RedisConf> getConfIssueRedises() {
        return confIssueRedises;
    }

    public void setConfIssueRedises(Set<RedisConf> confIssueRedises) {
        this.confIssueRedises = confIssueRedises;
    }

    public boolean isRedisVersionCollected() {
        return redisVersionCollected;
    }

    public void setRedisVersionCollected(boolean redisVersionCollected) {
        this.redisVersionCollected = redisVersionCollected;
    }

    public boolean isRedisConfCollected() {
        return redisConfCollected;
    }

    public void setRedisConfCollected(boolean redisConfCollected) {
        this.redisConfCollected = redisConfCollected;
    }
}
