package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.api.email.EmailResponse;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.checker.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.dao.ClusterDao;
import com.ctrip.xpipe.redis.console.dao.ConfigDao;
import com.ctrip.xpipe.redis.console.dao.RedisDao;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.impl.AlertEventService;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;

import java.util.*;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.console.service.ConfigService.*;


public class DefaultPersistenceCache extends AbstractPersistenceCache{
    
    private ClusterDao clusterDao;

    private RedisDao redisDao;
    
    private DcClusterShardService dcClusterShardService;
    
    private ConfigDao configDao;
    
    private AlertEventService alertEventService;

    public DefaultPersistenceCache(CheckerConfig config, 
                                   AlertEventService alertEventService,
                                   ConfigDao configDao,
                                   DcClusterShardService dcClusterShardService,
                                   RedisDao redisDao,
                                   ClusterDao clusterDao) {
        super(config);
        this.alertEventService = alertEventService;
        this.configDao = configDao;
        this.dcClusterShardService = dcClusterShardService;
        this.redisDao = redisDao;
        this.clusterDao = clusterDao;
    }

    @Override
    public void updateRedisRole(RedisHealthCheckInstance instance, Server.SERVER_ROLE role) {
        if (!Server.SERVER_ROLE.MASTER.equals(role) && !Server.SERVER_ROLE.SLAVE.equals(role)) {
            // only handle role as master or slave
            return;
        }

        RedisInstanceInfo info = instance.getCheckInfo();
        String dcId = info.getDcId();
        String clusterId = info.getClusterId();
        String shardId = info.getShardId();

        DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.find(dcId, clusterId, shardId);
        if (null == dcClusterShardTbl) {
            logger.warn("[updateRedisRole][{}] no {}-{}-{} ", info.getHostPort(), dcId, clusterId, shardId);
            return;
        }

        List<RedisTbl> redises = redisDao.findAllByDcClusterShard(dcClusterShardTbl.getDcClusterShardId(), XPipeConsoleConstant.ROLE_REDIS);
        if (null == redises) {
            logger.warn("[updateRedisRole][{}] no redises for {}", info.getHostPort(), dcClusterShardTbl.getDcClusterShardId());
            return;
        }

        for(RedisTbl redis : redises) {
            if(redis.getRedisIp().equals(info.getHostPort().getHost())
                    && redis.getRedisPort() == info.getHostPort().getPort()) {
                logger.info("[updateRedisRole][{}] {}", info.getHostPort(), role);
                redis.setMaster(Server.SERVER_ROLE.MASTER.equals(role));
                info.isMaster(redis.isMaster());
                redisDao.updateBatchMaster(Lists.newArrayList(redis));
                break;
            }
        }
    }
    
    @VisibleForTesting
    @SuppressWarnings("unchecked")
    protected EventModel createEventModel(String eventOperator, AlertMessageEntity message, EmailResponse response) {
        EventModel model = new EventModel();
        model.setEventType(EventModel.EventType.ALERT_EMAIL).setEventOperator(eventOperator)
                .setEventDetail(message.getTitle());
        if(message.getAlert() != null) {
            model.setEventOperation(message.getAlert().getAlertType().name());
        } else {
            model.setEventOperation("grouped");
        }
        String emailCheckInfo = null;
        try {
            emailCheckInfo = JsonCodec.INSTANCE.encode(response.getProperties());
        } catch (Exception e) {
            logger.error("[createEventModel] Error encode check info");
        }
        model.setEventProperty(emailCheckInfo);
        return model;
    }
    
    @Override
    public void recordAlert(String eventOperator, AlertMessageEntity message, EmailResponse response) {
        EventModel model = createEventModel(eventOperator, message, response);
        alertEventService.insert(model);
    }

    private Set<String> findConfigWhiteList(String key) {
        Set<String> whiteList = new HashSet<>();
        List<ConfigTbl> configTbls = configDao.findAllByKeyAndValueAndUntilAfter(key, String.valueOf(true), new Date());
        if (null == configTbls) {
            logger.debug("[findConfigWhiteList][{}] no such config", key);
            return whiteList;
        }

        configTbls.forEach(configTbl -> whiteList.add(configTbl.getSubKey()));
        return whiteList;
    }
    
    @Override
    Set<String> doSentinelCheckWhiteList() {
        return findConfigWhiteList(KEY_SENTINEL_CHECK_EXCLUDE);
    }

    @Override
    Set<String> doClusterAlertWhiteList() {
        return findConfigWhiteList(KEY_CLUSTER_ALERT_EXCLUDE);
    }

    @Override
    Set<String> doGetMigratingClusterList() {
        return clusterDao.findMigratingClusterNames()
                .stream().map(ClusterTbl::getClusterName)
                .collect(Collectors.toSet());
    }

    private boolean isConfigOnOrExpired(String key, boolean defaultVal) {
        try {
            ConfigTbl config = configDao.getByKey(key);
            boolean value = Boolean.parseBoolean(config.getValue());
            Date expireDate = config.getUntil();
            if ((new Date()).after(expireDate)) return defaultVal;
            return value;
        } catch (Throwable th) {
            logger.info("[isSentinelAutoProcess] fail", th);
            return defaultVal;
        }
    }

    private boolean isConfigOnOrExpired(String key) {
        return isConfigOnOrExpired(key, true);
    }
    
    @Override
    boolean doIsSentinelAutoProcess() {
        return isConfigOnOrExpired(KEY_SENTINEL_AUTO_PROCESS);
    }

    @Override
    boolean doIsAlertSystemOn() {
        return isConfigOnOrExpired(KEY_ALERT_SYSTEM_ON);
    }

    @Override
    Map<String, Date> doLoadAllClusterCreateTime() {
        Map<String, Date> clusterCreateTimes = new HashMap<>();
        List<ClusterTbl> clusterTbls = clusterDao.findAllClustersWithCreateTime();
        for(ClusterTbl clusterTbl : clusterTbls) {
            clusterCreateTimes.put(clusterTbl.getClusterName(), clusterTbl.getCreateTime());
        }

        return clusterCreateTimes;
    }
    
}
