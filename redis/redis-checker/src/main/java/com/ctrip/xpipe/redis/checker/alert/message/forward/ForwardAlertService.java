package com.ctrip.xpipe.redis.checker.alert.message.forward;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.checker.alert.message.AlertEntityHandler;
import com.ctrip.xpipe.redis.checker.alert.message.AlertEntityHolderManager;
import com.ctrip.xpipe.redis.checker.alert.message.holder.DefaultAlertEntityHolderManager;
import com.ctrip.xpipe.redis.checker.alert.policy.receiver.EmailReceiverModel;
import com.google.common.collect.Sets;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author qifanwang
 * date 2024/6/18
 */

@Service
public class ForwardAlertService extends AlertEntityHandler {

    protected static final Logger logger = LoggerFactory.getLogger(ForwardAlertService.class);

    private final static int QUEUE_LENGTH = 4096;

    private final static long REPORT_INTERVAL = 1 * 60 * 1000;

    private final static long ALERT_CACHE_TIME = 5 * 60 * 1000;

    private final static int CACHE_LIMIT = 1024;

    private final static int MAX_MAIL_MERGE_BATH = 1024;

    Map<AlertEntity, Long> alertCache;
    Map<AlertEntity, Long> recoverCache;

    private BlockingQueue<AlertEntity> alertQueue;
    private BlockingQueue<AlertEntity> recoverQueue;

    public ForwardAlertService() {
        this.alertQueue = new LinkedBlockingQueue<>();
        this.recoverQueue = new LinkedBlockingQueue<>();
    }

    @PostConstruct
    public void initService() {

        alertCache = new LRUCache<>(CACHE_LIMIT, ALERT_CACHE_TIME);
        recoverCache = new LRUCache<>(CACHE_LIMIT, ALERT_CACHE_TIME);

        scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                tryMergeAlert(true);
            }
        }, 0, REPORT_INTERVAL, TimeUnit.MILLISECONDS);

        scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                tryMergeAlert(false);
            }
        },0, REPORT_INTERVAL, TimeUnit.MILLISECONDS);
    }


    private void tryMergeAlert(boolean isAlert) {
        try {
            mergeAlertMultiples(isAlert);
        } catch (Throwable th) {
            // avoid thread exit
            getLogger().error("[tryMergeAlert]" + th.getMessage(), th);
        }
    }



    private void mergeAlertMultiples(boolean isAlert) {
        boolean status = true;
        while (status) {
            status = mergeAlert(isAlert);
        }
    }

    private boolean mergeAlert(boolean isAlert) {

        AlertEntityHolderManager holderManager = new DefaultAlertEntityHolderManager();

        BlockingQueue<AlertEntity> entitiesQueue = getAlertQueue(isAlert);

        Set<AlertEntity> sendingAlerts = fillSendingAlerts(entitiesQueue, isAlert);
        if(sendingAlerts.size() == 0) {
            return false;
        }

        addAlertsToAlertHolders(sendingAlerts, holderManager);

        Map<EmailReceiverModel, Map<ALERT_TYPE, Set<AlertEntity>>> map = alertPolicyManager().queryGroupedEmailReceivers(holderManager);

        for(Map.Entry<EmailReceiverModel, Map<ALERT_TYPE, Set<AlertEntity>>> mailGroup : map.entrySet()) {
            if(mailGroup.getValue() == null || mailGroup.getValue().isEmpty()) {
                continue;
            }
            AlertMessageEntity message = getMessage(mailGroup.getKey(), mailGroup.getValue(), isAlert);
            emailMessage(message);
            tryMetric(mailGroup.getValue(), isAlert);
        }
        return true;
    }

    private Set<AlertEntity> fillSendingAlerts(BlockingQueue<AlertEntity> entitiesQueue, boolean isAlert) {
        int count = 0;
        Set<AlertEntity> sendingAlerts = Sets.newConcurrentHashSet();
        while(!entitiesQueue.isEmpty() && count < MAX_MAIL_MERGE_BATH) {
            AlertEntity entity = entitiesQueue.poll();
            Map<AlertEntity, Long> cache = getAlertCache(isAlert);
            if(entity != null && !cache.containsKey(entity)) {
                sendingAlerts.add(entity);
                cache.put(entity, System.currentTimeMillis());
            }
            count++;
        }
        return sendingAlerts;
    }

    private BlockingQueue<AlertEntity> getAlertQueue(boolean isAlert) {
        if(isAlert) {
            return alertQueue;
        } else {
            return recoverQueue;
        }
    }

    private Map<AlertEntity, Long> getAlertCache(boolean isAlert) {
        if(isAlert) {
            return alertCache;
        } else {
            return recoverCache;
        }
    }

    public boolean addAll(boolean isAlert, List<AlertEntity> alertEntities) {
        BlockingQueue<AlertEntity> queue = getAlertQueue(isAlert);
        if(queue.size() > QUEUE_LENGTH) {
            logger.info("[addAlertEntities] full");
            // queue is full, can not add
            return false;
        }

        for(AlertEntity alertEntity : alertEntities) {
            queue.offer(alertEntity);
        }
        return true;
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    private class LRUCache<K, V> extends LinkedHashMap<K, V> {
        private final int cacheLimit;
        private final long cacheTime;

        public LRUCache(int cacheLimit, long cacheTime) {
            super(cacheLimit + 1, 1.0f, true); // 设置accessOrder为true以实现LRU
            this.cacheLimit = cacheLimit;
            this.cacheTime = cacheTime;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > cacheLimit;
        }

        @Override
        public boolean containsKey(Object key) {
            V value = get(key);
            if (value == null) {
                return false;
            }
            return System.currentTimeMillis() - ((Long) value) < cacheTime;
        }
    }
}
