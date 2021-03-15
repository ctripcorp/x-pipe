package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.Persistence;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.dao.ClusterDao;
import com.ctrip.xpipe.redis.console.dao.ConfigDao;
import com.ctrip.xpipe.redis.console.dao.RedisDao;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

import static com.ctrip.xpipe.redis.console.service.ConfigService.*;

/**
 * @author lishanglin
 * date 2021/3/9
 */
@Component
public class DefaultPersistence implements Persistence {

    @Autowired
    private ClusterDao clusterDao;

    @Autowired
    private RedisDao redisDao;

    @Autowired
    private DcClusterShardService dcClusterShardService;

    @Autowired
    private ConfigDao configDao;

    private static Logger logger = LoggerFactory.getLogger(DefaultPersistence.class);

    @Override
    public boolean isClusterOnMigration(String clusterId) {
        ClusterTbl clusterTbl = clusterDao.findClusterByClusterName(clusterId);
        if (null == clusterTbl) return false;

        return ClusterStatus.isSameClusterStatus(clusterTbl.getStatus(), ClusterStatus.Normal);
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

    @Override
    public Set<String> sentinelCheckWhiteList() {
        Set<String> whiteList = new HashSet<>();
        List<ConfigTbl> configTbls = configDao.findAllByKeyAndValueAndUntilAfter(
                KEY_SENTINEL_CHECK_EXCLUDE, String.valueOf(true), new Date());
        if (null == configTbls) {
            logger.info("[sentinelCheckWhiteList] no such config");
            return whiteList;
        }

        configTbls.forEach(configTbl -> whiteList.add(configTbl.getSubKey()));
        return whiteList;
    }

    @Override
    public boolean isSentinelAutoProcess() {
        return isConfigOnOrExpired(KEY_SENTINEL_AUTO_PROCESS);
    }

    @Override
    public boolean isAlertSystemOn() {
        return isConfigOnOrExpired(KEY_ALERT_SYSTEM_ON);
    }

    private boolean isConfigOnOrExpired(String key) {
        try {
            ConfigTbl config = configDao.getByKey(key);
            boolean value = Boolean.parseBoolean(config.getValue());
            Date expireDate = config.getUntil();
            return value || (new Date()).after(expireDate);
        } catch (Throwable th) {
            logger.info("[isSentinelAutoProcess] fail", th);
            return true;
        }
    }


    @Override
    public Date getClusterCreateTime(String clusterId) {
        ClusterTbl clusterTbl = clusterDao.findClusterByClusterName(clusterId);
        if (null == clusterTbl) return null;

        return clusterTbl.getCreateTime();
    }



    @Override
    public Map<String, Date> loadAllClusterCreateTime() {
        Map<String, Date> clusterCreateTimes = new HashMap<>();
        // TODO: 优化返回字段，只需要createdTime @sl_li
        List<ClusterTbl> clusterTbls = clusterDao.findAllClusters();
        for(ClusterTbl clusterTbl : clusterTbls) {
            clusterCreateTimes.put(clusterTbl.getClusterName(), clusterTbl.getCreateTime());
        }

        return clusterCreateTimes;
    }

}
