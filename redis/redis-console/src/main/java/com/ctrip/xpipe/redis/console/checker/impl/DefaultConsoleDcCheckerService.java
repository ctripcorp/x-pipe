package com.ctrip.xpipe.redis.console.checker.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.checker.model.CheckerRole;
import com.ctrip.xpipe.redis.console.checker.ConsoleCheckerGroupService;
import com.ctrip.xpipe.redis.console.checker.ConsoleDcCheckerService;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.model.InstanceCheckerHealthCheckModel;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.model.ShardCheckerHealthCheckModel;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.ctrip.xpipe.redis.checker.resource.Resource.REDIS_COMMAND_EXECUTOR;

@Component
public class DefaultConsoleDcCheckerService implements ConsoleDcCheckerService {

    @Autowired
    public ConsoleServiceManager consoleManager;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private DcService dcService;

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private ConsoleCheckerGroupService consoleCheckerGroupService;

    private static final String currentDc = FoundationService.DEFAULT.getDataCenter();

    @Override
    public List<ShardCheckerHealthCheckModel> getShardAllCheckerGroupHealthCheck(String dcId, String clusterId, String shardId) {
        List<ShardCheckerHealthCheckModel> result = new ArrayList<>();
        ClusterTbl clusterTbl = clusterService.find(clusterId);
        if (clusterTbl == null) {
            return null;
        }
        String activeDc = dcService.getDcName(clusterTbl.getActivedcId());
        if (metaCache.isCrossRegion(dcId, activeDc)) {
            result.addAll(consoleManager.getShardAllCheckerGroupHealthCheck(dcId, dcId, clusterId, shardId));
        }
        if (activeDc.equalsIgnoreCase(currentDc)) {
            return getLocalDcShardAllCheckerGroupHealthCheck(dcId, clusterId, shardId);
        }
        result.addAll(consoleManager.getShardAllCheckerGroupHealthCheck(activeDc, dcId, clusterId, shardId));
        return result;
    }

    @Override
    public List<ShardCheckerHealthCheckModel> getLocalDcShardAllCheckerGroupHealthCheck(String dcId, String clusterId, String shardId) {
        ClusterTbl clusterTbl = clusterService.find(clusterId);
        if (clusterTbl == null) {
            return null;
        }
        List<RedisMeta> redisOfDcClusterShard = metaCache.getRedisOfDcClusterShard(dcId, clusterId, shardId);
        Map<HostPort, CommandFuture<Map<HostPort, String>>> redisCheckerActionMap = new HashMap<>();
        Map<HostPort, CommandFuture<Map<HostPort, HEALTH_STATE>>> redisCheckerHealthStateMap = new HashMap<>();
        redisOfDcClusterShard.forEach(redisMeta -> {
            boolean isCrossRegion = metaCache.isCrossRegion(currentDc, dcService.getDcName(clusterTbl.getActivedcId()));
            CommandFuture<Map<HostPort, String>> allHealthCheckInstance = consoleCheckerGroupService.getAllHealthCheckInstance(clusterTbl.getId(), redisMeta.getIp(), redisMeta.getPort(), isCrossRegion);
            CommandFuture<Map<HostPort, HEALTH_STATE>> allHealthStates = consoleCheckerGroupService.getAllHealthStates(clusterTbl.getId(), redisMeta.getIp(), redisMeta.getPort(), isCrossRegion);
            redisCheckerActionMap.put(new HostPort(redisMeta.getIp(), redisMeta.getPort()), allHealthCheckInstance);
            redisCheckerHealthStateMap.put(new HostPort(redisMeta.getIp(), redisMeta.getPort()), allHealthStates);
        });
        Map<HostPort, Map<HostPort, InstanceCheckerHealthCheckModel>> checkerInstancesMap = new HashMap<>();
        generateResult(redisCheckerActionMap, checkerInstancesMap, String.class);
        generateResult(redisCheckerHealthStateMap, checkerInstancesMap, HEALTH_STATE.class);

        HostPort checkerLeader = consoleCheckerGroupService.getCheckerLeader(clusterTbl.getId());
        List<ShardCheckerHealthCheckModel> result = new ArrayList<>();
        for (Map.Entry<HostPort, Map<HostPort, InstanceCheckerHealthCheckModel>> entry : checkerInstancesMap.entrySet()) {
            HostPort checker = entry.getKey();
            ShardCheckerHealthCheckModel shardCheckerHealthCheckModel = new ShardCheckerHealthCheckModel(checker.getHost(), checker.getPort(), currentDc);
            if (checker.equals(checkerLeader)) {
                shardCheckerHealthCheckModel.setCheckerRole(CheckerRole.LEADER);
            }
            shardCheckerHealthCheckModel.setInstances(new ArrayList<>(entry.getValue().values()));
            result.add(shardCheckerHealthCheckModel);
        }
        return result;
    }

    private <T> void generateResult(Map<HostPort, CommandFuture<Map<HostPort, T>>> futureMap, Map<HostPort, Map<HostPort, InstanceCheckerHealthCheckModel>> result, Class<T> clazz) {
        for (Map.Entry<HostPort, CommandFuture<Map<HostPort, T>>> entry : futureMap.entrySet()) {
            HostPort redis = entry.getKey();
            Map<HostPort, T> checkerHealthMap = getFuture(entry.getValue());
            if (checkerHealthMap == null) continue;
            for (Map.Entry<HostPort, T> checkerActionEntry : checkerHealthMap.entrySet()) {
                HostPort checker = checkerActionEntry.getKey();
                if (!result.containsKey(checker)) {
                    result.put(new HostPort(checker.getHost(), checker.getPort()), new HashMap<>());
                }
                if (!result.get(checker).containsKey(redis)) {
                    result.get(checker).put(redis, new InstanceCheckerHealthCheckModel().setHost(redis.getHost()).setPort(redis.getPort()));
                }
                InstanceCheckerHealthCheckModel model = result.get(checker).get(redis);
                if (clazz == String.class) {
                    model.setActions((String) checkerActionEntry.getValue());
                } else if (clazz == HEALTH_STATE.class) {
                    model.setState((HEALTH_STATE) checkerActionEntry.getValue());
                }
            }
        }
    }

    private <T> T getFuture(CommandFuture<T> future) {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            return null;
        }
    }

}
