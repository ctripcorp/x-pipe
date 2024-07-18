package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayAction;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayActionListener;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStateService;
import com.ctrip.xpipe.redis.checker.impl.CheckerRedisDelayManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.model.consoleportal.UnhealthyInfoModel;
import com.ctrip.xpipe.redis.console.service.CrossMasterDelayService;
import com.ctrip.xpipe.redis.console.service.DelayService;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;

/**
 * @author chen.zhu
 * <p>
 * Sep 03, 2018
 */
@Component
public class DefaultDelayService extends CheckerRedisDelayManager implements DelayService, DelayActionListener, OneWaySupport, BiDirectionSupport {

    private static final Logger logger = LoggerFactory.getLogger(DefaultDelayService.class);

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private ConsoleServiceManager consoleServiceManager;

    @Autowired
    private CrossMasterDelayService crossMasterDelayService;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private HealthStateService healthStateService;
    
    @Autowired
    private FoundationService foundationService;

    @Resource(name = GLOBAL_EXECUTOR)
    protected ExecutorService executors;

    @Override
    public void updateRedisDelays(Map<HostPort, Long> redisDelays) {
        hostPort2Delay.putAll(redisDelays);
    }

    @Override
    public void updateHeteroShardsDelays(Map<Long, Long> heteroShardsDelays) {
        heteroShardsDelay.putAll(heteroShardsDelays);
    }

    @Override
    public long getShardDelay(String clusterId, String shardId, Long shardDbId) {
        String dcId = metaCache.getActiveDc(clusterId);

        if (StringUtil.isEmpty(dcId)) {
            return -1L;
        }

        long result;
        if(!foundationService.getDataCenter().equals(dcId)) {
            try {
                result = consoleServiceManager.getShardDelay(shardDbId, dcId);
            } catch (Exception e) {
                return -1L;
            }
        } else {
            result = heteroShardsDelay.getOrDefault(shardDbId, DelayAction.SAMPLE_LOST_AND_NO_PONG);
        }
        return TimeUnit.NANOSECONDS.toMillis(result);
    }

    @Override
    public long getDelay(HostPort hostPort) {
        Pair<String, String> clusterShard = metaCache.findClusterShard(hostPort);
        if (null == clusterShard) return -1L;

        ClusterType clusterType = metaCache.getClusterType(clusterShard.getKey());
        ClusterType azGroupType = metaCache.getAzGroupType(hostPort);
        String dcId = null;
        if (clusterType.supportSingleActiveDC() && azGroupType != ClusterType.SINGLE_DC) {
            dcId = metaCache.getActiveDc(hostPort);
        } else if (clusterType.supportMultiActiveDC() || azGroupType == ClusterType.SINGLE_DC) {
            dcId = metaCache.getDc(hostPort);
        }

        if (StringUtil.isEmpty(dcId)) {
            return -1L;
        }

        long result;
        if(!foundationService.getDataCenter().equals(dcId)) {
            try {
                result = consoleServiceManager.getDelay(hostPort.getHost(), hostPort.getPort(), dcId);
            } catch (Exception e) {
                return -1L;
            }
        } else {
            result = hostPort2Delay.getOrDefault(hostPort, DelayAction.SAMPLE_LOST_AND_NO_PONG);
        }  
        return TimeUnit.NANOSECONDS.toMillis(result);
    }

    @Override
    public long getDelay(ClusterType clusterType, HostPort hostPort) {
        if (consoleConfig.getOwnClusterType().contains(clusterType.toString())) {
            return getDelay(hostPort);
        } else {
            return consoleServiceManager.getDelayFromParallelService(hostPort.getHost(), hostPort.getPort());
        }

    }

    @Override
    public long getLocalCachedDelay(HostPort hostPort) {
        return hostPort2Delay.getOrDefault(hostPort, DelayAction.SAMPLE_LOST_AND_NO_PONG);
    }

    @Override
    public long getLocalCachedShardDelay(long shardId) {
        return heteroShardsDelay.getOrDefault(shardId, DelayAction.SAMPLE_LOST_AND_NO_PONG);
    }

    @Override
    public Map<HostPort, Long> getDcCachedDelay(String dc) {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (null == xpipeMeta) return Collections.emptyMap();

        if (!foundationService.getDataCenter().equalsIgnoreCase(dc)) {
            try {
                return consoleServiceManager.getAllDelay(dc);
            } catch (Exception e) {
                return Collections.emptyMap();
            }
        }

        Map<HostPort, Long> localDelayMap = new HashMap<>(hostPort2Delay);
        for (String dcId : xpipeMeta.getDcs().keySet()) {
            for (HostPort redis : metaCache.getAllActiveRedisOfDc(foundationService.getDataCenter(), dcId)) {
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

        if (!foundationService.getDataCenter().equalsIgnoreCase(dc)) {
            try {
                return consoleServiceManager.getUnhealthyInstanceByIdc(dc);
            } catch (Exception e) {
                logger.debug("[getDcActiveClusterUnhealthyInstance][{}] request remote console fail", dc, e);
                return null;
            }
        }

        String currentIdc = foundationService.getDataCenter();
        Map<HostPort, HEALTH_STATE> cachedHealthStatus = healthStateService.getAllCachedState();
        UnhealthyInfoModel unhealthyInfo = new UnhealthyInfoModel();
        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {

            for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
                if (!clusterType.supportHealthCheck()) continue;
                if (clusterType.supportSingleActiveDC() && !clusterMeta.getActiveDc().equalsIgnoreCase(currentIdc)) continue;
                if (clusterType.supportMultiActiveDC() && !dcMeta.getId().equalsIgnoreCase(currentIdc)) continue;

                for (ShardMeta shardMeta : clusterMeta.getShards().values()) {

                    for (RedisMeta redisMeta : shardMeta.getRedises()) {
                        HostPort hostPort = new HostPort(redisMeta.getIp(), redisMeta.getPort());
                        if (!cachedHealthStatus.containsKey(hostPort)) continue;

                        HEALTH_STATE state = cachedHealthStatus.get(hostPort);
                        if(HEALTH_STATE.DOWN.equals(state) || HEALTH_STATE.SICK.equals(state)) {
                            unhealthyInfo.addUnhealthyInstance(clusterMeta.getId(), dcMeta.getId(), shardMeta.getId(), hostPort, redisMeta.isMaster());
                        }
                    }

                }
            }
        }

        UnhealthyInfoModel unhealthyMaster = crossMasterDelayService.getCurrentDcUnhealthyMasters();
        unhealthyInfo.merge(unhealthyMaster);

        return unhealthyInfo;
    }

    @Override
    public UnhealthyInfoModel getAllUnhealthyInstance() {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if(xpipeMeta == null || xpipeMeta.getDcs() == null) {
            return null;
        }

        UnhealthyInfoModel infoAggregation = new UnhealthyInfoModel();
        ParallelCommandChain commandChain = new ParallelCommandChain(executors);
        Map<String, CommandFuture<UnhealthyInfoModel>> results = new HashMap<>();
        for (String dcId : xpipeMeta.getDcs().keySet()) {
            FetchDcUnhealthyInstanceCmd cmd = new FetchDcUnhealthyInstanceCmd(dcId);
            commandChain.add(cmd);
            results.put(dcId, cmd.future());
        }

        try {
            commandChain.execute().sync();
            for (Map.Entry<String, CommandFuture<UnhealthyInfoModel>> result: results.entrySet()) {
                CommandFuture<UnhealthyInfoModel> commandFuture = result.getValue();
                if (commandFuture.isSuccess() && null != commandFuture.get()) {
                    infoAggregation.merge(commandFuture.get());
                } else {
                    infoAggregation.getAttachFailDc().add(result.getKey());
                }
            }
        } catch (Throwable th) {
            logger.info("[getAllUnhealthyInstance][fail] {}", th.getMessage());
        }
        return infoAggregation;
    }

    @Override
    public UnhealthyInfoModel getAllUnhealthyInstanceFromParallelService() {
        return consoleServiceManager.getAllUnhealthyInstanceFromParallelService();
    }
    
    @VisibleForTesting
    public void setFoundationService(FoundationService foundationService) {
        this.foundationService = foundationService;
    }

    class FetchDcUnhealthyInstanceCmd extends AbstractCommand<UnhealthyInfoModel> {

        private String dc;

        public FetchDcUnhealthyInstanceCmd(String dc) {
            this.dc = dc;
        }

        @Override
        protected void doExecute() throws Throwable {
            future().setSuccess(getDcActiveClusterUnhealthyInstance(dc));
        }

        @Override
        protected void doReset() {
            // do nothing
        }

        @Override
        public String getName() {
            return getClass().getSimpleName();
        }
    }

}
