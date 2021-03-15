package com.ctrip.xpipe.redis.checker.alert;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.Persistence;
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
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

//    @Autowired
//    private ClusterService clusterService;

    @Autowired
    private Persistence persistence;

    @Autowired
    private AlertConfig alertConfig;

    @Autowired
    private NotificationManager notifier;

    @Resource(name = SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Autowired
    private MetaCache metaCache;

    private Set<String> alertClusterWhiteList;

    private Map<String, Date> clusterCreateTime = new HashMap<>();

    @PostConstruct
    public void postConstruct(){

        scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() throws Exception {
                alertClusterWhiteList = alertConfig.getAlertWhileList();
            }
        }, 0, 30, TimeUnit.SECONDS);

        scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                logger.info("[clusterCreateTimeMapper][execute]");
//                List<ClusterTbl> clusterTbls = clusterService.findAllClustersWithOrgInfo();
//                for(ClusterTbl clusterTbl : clusterTbls) {
//                    clusterCreateTime.put(clusterTbl.getClusterName(), clusterTbl.getCreateTime());
//                }
                clusterCreateTime = persistence.loadAllClusterCreateTime();
            }
        }, 1, 60, TimeUnit.MINUTES);

    }

    private Date getClusterCreateTime(String clusterName) {
        if(StringUtil.isEmpty(clusterName)) {
            logger.error("[getClusterCreateTime] empty cluster name");
            return null;
        }
        Date date = clusterCreateTime.get(clusterName);
        if(date == null) {
            date = persistence.getClusterCreateTime(clusterName);
            if (null == date) {
                logger.error("[getClusterCreateTime] cluster create time not found: {}", clusterName);
            } else {
                clusterCreateTime.put(clusterName, date);
            }

//            ClusterTbl clusterTbl = clusterService.find(clusterName);
//            if(clusterTbl == null) {
//                logger.error("[getClusterCreateTime] cluster not found: {}", clusterName);
//            } else {
//                date = clusterTbl.getCreateTime();
//                if(date != null) {
//                    clusterCreateTime.put(clusterName, date);
//                }
//            }
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
//            logger.warn("[alert][skip]{}, {}, {}, {}", cluster, shard, type, message);
            return;
        }


//        logger.warn("[alert]{}, {}, {}, {}", cluster, shard, type, message);
        EventMonitor.DEFAULT.logEvent(ALERT_TYPE, generateAlertMessage(dc, cluster, shard, type, message));
        notifier.addAlert(dc, cluster, shard, hostPort, type, message);
    }

    @VisibleForTesting
    public boolean shouldAlert(String cluster) {
        try {
            Date createTime = getClusterCreateTime(cluster);
            int minutes = alertConfig.getNoAlarmMinutesForClusterUpdate();
            Date current = new Date();
            if (createTime != null && current.before(DateTimeUtils.getMinutesLaterThan(createTime, minutes))) {
                return false;
            }
        } catch (Exception e) {
            logger.warn("[shouldAlert]", e);
        }
        return !alertClusterWhiteList.contains(cluster);
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
    protected void setClusterCreateTime(Map<String, Date> map) {
        this.clusterCreateTime = map;
    }

    @VisibleForTesting
    protected void setAlertClusterWhiteList(Set<String> clusterWhiteList) {
        this.alertClusterWhiteList = clusterWhiteList;
    }
}
