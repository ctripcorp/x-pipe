package com.ctrip.xpipe.redis.checker.alert;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.alert.manager.NotificationManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 16, 2017
 */
@Component
public class AlertManager {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private static final String ALERT_TYPE = "Notification";

    @Autowired
    private PersistenceCache persistenceCache;

    @Autowired
    private AlertConfig alertConfig;

    @Autowired
    private NotificationManager notifier;

    @Autowired
    private AlertDbConfig alertDbConfig;

    @Resource(name = SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Autowired
    private MetaCache metaCache;

    private Set<String> alertClusterWhiteList;

//    private Map<String, Date> clusterCreateTime = new HashMap<>();

    @PostConstruct
    public void postConstruct(){

        scheduled.scheduleWithFixedDelay(this::refreshWhiteList, 0, 30, TimeUnit.SECONDS);

    }

    @VisibleForTesting
    protected void refreshWhiteList() {
        Set<String> whiteList = new HashSet<>();
        whiteList.addAll(persistenceCache.clusterAlertWhiteList());
        whiteList.addAll(alertConfig.getAlertWhileList().stream().map(String::toLowerCase).collect(Collectors.toList()));
        this.alertClusterWhiteList = whiteList;
    }

    private Date getClusterCreateTime(String clusterName) {
        if(StringUtil.isEmpty(clusterName)) {
            logger.error("[getClusterCreateTime] empty cluster name");
            return null;
        }
        Date date = persistenceCache.getClusterCreateTime(clusterName);
        if (null == date) {
            logger.error("[getClusterCreateTime] cluster create time not found: {}", clusterName);
        }
        return date;
    }

    public void alert(String dc, String cluster, String shard, HostPort hostPort, ALERT_TYPE type, String message) {
        doAlert(dc, cluster, shard, hostPort, type, message, false);
    }

    public void alert(RedisInstanceInfo info, ALERT_TYPE type, String message) {
        doAlert(info.getDcId(), info.getClusterId(), info.getShardId(), info.getHostPort(), type, message, false);
    }

    public void alert(String cluster, String shard, HostPort hostPort, ALERT_TYPE type, String message){

        String dc = findDc(hostPort);
        doAlert(dc, cluster, shard, hostPort, type, message, false);
    }

    private void doAlert(String dc, String cluster, String shard, HostPort hostPort, ALERT_TYPE type, String message, boolean force) {

        if(!force && !shouldAlert(cluster)){
            return;
        }


        EventMonitor.DEFAULT.logEvent(ALERT_TYPE, generateAlertMessage(dc, cluster, shard, type, message));
        notifier.addAlert(dc, cluster, shard, hostPort, type, message);
    }

    @VisibleForTesting
    public boolean shouldAlert(String cluster) {
        try {
            if (StringUtil.isEmpty(cluster)) return true;
            Date createTime = getClusterCreateTime(cluster);
            int minutes = alertConfig.getNoAlarmMinutesForClusterUpdate();
            Date current = new Date();
            if (createTime != null && current.before(DateTimeUtils.getMinutesLaterThan(createTime, minutes))) {
                return false;
            }
        } catch (Exception e) {
            logger.warn("[shouldAlert]", e);
        }
        return !alertClusterWhiteList.contains(cluster.toLowerCase());
    }

    private String findDc(HostPort hostPort) {
        try {
            if (hostPort != null && hostPort.getHost() != null) {
                return metaCache.getDc(hostPort);
            }
            return null;
        } catch (Exception e) {
            logger.warn("[findDc] error: ", e);
            return null;
        }
    }

    String generateAlertMessage(String dc, String cluster, String shard, ALERT_TYPE type, String message) {
        StringBuilder sb = new StringBuilder();
        if(dc != null && !dc.isEmpty()) {
            sb.append(dc).append(",");
        }
        if(cluster != null && !cluster.isEmpty()) {
            sb.append(cluster).append(",");
        }
        if(shard != null && !shard.isEmpty()) {
            sb.append(shard).append(",");
        }
        if(type != null) {
            sb.append(type.simpleDesc()).append(",");
        }
        if(message != null && !message.isEmpty()) {
            sb.append(message);
        } else {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    @VisibleForTesting
    protected void setAlertClusterWhiteList(Set<String> clusterWhiteList) {
        this.alertClusterWhiteList = clusterWhiteList;
    }
}
