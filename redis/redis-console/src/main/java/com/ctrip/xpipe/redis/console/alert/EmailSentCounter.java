package com.ctrip.xpipe.redis.console.alert;

import com.ctrip.xpipe.api.cluster.CrossDcClusterServer;
import com.ctrip.xpipe.api.email.EmailResponse;
import com.ctrip.xpipe.api.email.EmailService;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.checker.alert.sender.email.listener.AbstractEmailSenderCallback;
import com.ctrip.xpipe.redis.console.model.EventModel;
import com.ctrip.xpipe.redis.console.service.impl.AlertEventService;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.THREAD_POOL_TIME_OUT;

/**
 * @author chen.zhu
 * <p>
 * Mar 26, 2018
 */
@Component
public class EmailSentCounter extends AbstractEmailSenderCallback {

    @Autowired
    private AlertEventService alertEventService;

    @Autowired(required = false)
    private CrossDcClusterServer clusterServer;

    @PostConstruct
    public void scheduledCheckSentEmails() {
        logger.info("[scheduledCheckSentEmails] [post construct] begin");
        ScheduledExecutorService scheduled = MoreExecutors.getExitingScheduledExecutorService(
                new ScheduledThreadPoolExecutor(1, XpipeThreadFactory.create(getClass().getSimpleName() + "-")),
                THREAD_POOL_TIME_OUT, TimeUnit.SECONDS
        );
        start(scheduled);
    }

    private void start(ScheduledExecutorService scheduled) {

        long startTime = getStartTime();
        logger.info("[start] start in {} minutes", startTime);
        scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                if(clusterServer != null && !clusterServer.amILeader()) {
                    logger.info("[scheduledCheckSentEmails][not leader quit]");
                    return;
                }
                scheduledTask();
            }
        }, startTime, 60, TimeUnit.MINUTES);
    }

    @VisibleForTesting
    protected long getStartTime() {
        long deltaTime = TimeUnit.MINUTES.toMillis(1);
        long startTimeMilli = DateTimeUtils.getNearestHour().getTime() - System.currentTimeMillis() + deltaTime;
        return TimeUnit.MILLISECONDS.toMinutes(startTimeMilli);
    }

    @VisibleForTesting
    protected void scheduledTask() {
        logger.info("[scheduledTask] start retrieving info");
        int totalCount, successCount, failCount;

        List<EventModel> events = alertEventService.getLastHourAlertEvent();
        totalCount = events.size();
        Pair<Integer, Integer> successAndFail = statistics(events);
        successCount = successAndFail.getKey();
        failCount = successAndFail.getValue();

        logger.info("[scheduledTask] scheduled report, total email count: {}", totalCount);
        EventMonitor.DEFAULT.logEvent(EMAIL_SERVICE_CAT_TYPE,
                "total sent out", totalCount);
        EventMonitor.DEFAULT.logEvent(EMAIL_SERVICE_CAT_TYPE,
                "success sent out", successCount);
        EventMonitor.DEFAULT.logEvent(EMAIL_SERVICE_CAT_TYPE,
                "fail sent out", failCount);
    }

    @VisibleForTesting
    protected Pair<Integer, Integer> statistics(List<EventModel> events) {
        int success = 0, fail = 0;
        for(EventModel event : events) {
            String prop = event.getEventProperty();
            Properties properties = JsonCodec.INSTANCE.decode(prop, Properties.class);
            boolean successOrNot = EmailService.DEFAULT.checkAsyncEmailResult(new EmailResponse() {
                @Override
                public Properties getProperties() {
                    return properties;
                }
            });
            if(successOrNot) {
                success ++;
            } else {
                logger.info("[statistics] email fail sent out: {}", event.getEventProperty());
                fail ++;
            }
        }
        return new Pair<>(success, fail);
    }

    @Override
    public void success() {
    }

    @Override
    public void fail(Throwable throwable) {
    }

}
