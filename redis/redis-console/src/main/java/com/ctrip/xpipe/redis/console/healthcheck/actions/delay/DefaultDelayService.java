package com.ctrip.xpipe.redis.console.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.console.model.consoleportal.UnhealthyInfoModel;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Sep 03, 2018
 */
@Component
public class DefaultDelayService implements DelayService, DelayActionListener {

    private static final Logger logger = LoggerFactory.getLogger(DefaultDelayService.class);

    private ConcurrentMap<HostPort, Long> hostPort2Delay = Maps.newConcurrentMap();

    private static final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private ConsoleServiceManager consoleServiceManager;

    @Override
    public long getDelay(HostPort hostPort) {
        String dcId = metaCache.getActiveDc(hostPort);
        if (StringUtil.isEmpty(dcId)) {
            return -1L;
        }

        long result;
        if (!FoundationService.DEFAULT.getDataCenter().equalsIgnoreCase(dcId)) {
            result = consoleServiceManager.getDelay(hostPort.getHost(), hostPort.getPort(), dcId);
        } else {
            result = hostPort2Delay.getOrDefault(hostPort, DelayAction.SAMPLE_LOST_AND_NO_PONG);
        }
        return TimeUnit.NANOSECONDS.toMillis(result);
    }

    @Override
    public long getLocalCachedDelay(HostPort hostPort) {
        return hostPort2Delay.getOrDefault(hostPort, DelayAction.SAMPLE_LOST_AND_NO_PONG);
    }

    @Override
    public Map<HostPort, Long> getDcCachedDelay(String dc) {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (null == xpipeMeta) return Collections.emptyMap();

        if (!currentDcId.equalsIgnoreCase(dc)) {
            try {
                return consoleServiceManager.getAllDelay(dc);
            } catch (Exception e) {
                return Collections.emptyMap();
            }
        }

        Map<HostPort, Long> localDelayMap = new HashMap<>(hostPort2Delay);
        for (String dcId : xpipeMeta.getDcs().keySet()) {
            for (HostPort redis : metaCache.getAllRedisOfDc(currentDcId, dcId)) {
                if (!localDelayMap.containsKey(redis)) localDelayMap.put(redis, DelayAction.SAMPLE_LOST_AND_NO_PONG);
            }
        }

        return localDelayMap;
    }


    @Override
    public UnhealthyInfoModel getDcActiveClusterUnhealthyInstance(String dc) {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if(xpipeMeta == null || xpipeMeta.getDcs() == null) {
            return null;
        }

        if (!currentDcId.equalsIgnoreCase(dc)) {
            try {
                return consoleServiceManager.getUnhealthyInstanceByIdc(dc);
            } catch (Exception e) {
                return null;
            }
        }

        String currentIdc = FoundationService.DEFAULT.getDataCenter();
        UnhealthyInfoModel unhealthyInfo = new UnhealthyInfoModel();
        Map<HostPort, Long> redisDelayMap = getDcCachedDelay(currentIdc);
        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {

            for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                if (!clusterMeta.getActiveDc().equalsIgnoreCase(currentIdc)) continue;

                for (ShardMeta shardMeta : clusterMeta.getShards().values()) {

                    for (RedisMeta redisMeta : shardMeta.getRedises()) {
                        HostPort hostPort = new HostPort(redisMeta.getIp(), redisMeta.getPort());
                        Long delay = redisDelayMap.get(hostPort);
                        if(null != delay && (delay < 0 || delay == DelayAction.SAMPLE_LOST_BUT_PONG)) {
                            unhealthyInfo.addUnhealthyInstance(clusterMeta.getId(), dcMeta.getId(), shardMeta.getId(), hostPort);
                        }
                    }

                }
            }
        }

        return unhealthyInfo;
    }

    @Override
    public UnhealthyInfoModel getAllUnhealthyInstance() {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if(xpipeMeta == null || xpipeMeta.getDcs() == null) {
            return null;
        }

        UnhealthyInfoModel infoAggregation = new UnhealthyInfoModel();
        for (String dcId : xpipeMeta.getDcs().keySet()) {
            UnhealthyInfoModel unhealthyInfo = getDcActiveClusterUnhealthyInstance(dcId);
            if (null == unhealthyInfo) infoAggregation.getAttachFailDc().add(dcId);
            else infoAggregation.merge(unhealthyInfo);
        }

        return infoAggregation;
    }

    @Override
    public void onAction(DelayActionContext delayActionContext) {
        hostPort2Delay.put(delayActionContext.instance().getRedisInstanceInfo().getHostPort(),
                delayActionContext.getResult());
    }

    @Override
    public void stopWatch(HealthCheckAction action) {
        hostPort2Delay.remove(action.getActionInstance().getRedisInstanceInfo().getHostPort());
    }
}
