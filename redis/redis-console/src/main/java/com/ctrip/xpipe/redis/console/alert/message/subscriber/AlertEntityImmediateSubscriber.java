package com.ctrip.xpipe.redis.console.alert.message.subscriber;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.AlertMessageEntity;
import com.google.common.collect.Sets;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */

@Component
public class AlertEntityImmediateSubscriber extends AbstractAlertEntitySubscriber {

    private Set<AlertEntity> sentOnce = Sets.newTreeSet(new Comparator<AlertEntity>() {
        @Override
        public int compare(AlertEntity o1, AlertEntity o2) {
            return o1.getDate().compareTo(o2.getDate());
        }
    });

    private Lock lock = new ReentrantLock();

    @PostConstruct
    public void initCleaner() {
        scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                try {
                    lock.lock();
                    sentOnce.removeIf(alert -> alertRecovered(alert));
                } finally {
                    lock.unlock();
                }

            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    @Override
    protected void doProcessAlert(AlertEntity alert) {

        if(hasBeenSentOut(alert)) {
            return;
        }

        AlertMessageEntity message = getMessage(alert, true);

        emailMessage(message);

    }

    private boolean hasBeenSentOut(AlertEntity alert) {

        try {
            lock.lock();
            boolean result = sentOnce.contains(alert);

            // replace the old alert (update alert time)
            if (result) {
                sentOnce.remove(alert);
            }
            sentOnce.add(alert);

            return result;
        } finally {
            lock.unlock();
        }
    }
}
