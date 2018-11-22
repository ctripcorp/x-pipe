package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.ProxyChainModel;
import com.ctrip.xpipe.redis.console.model.ProxyPingStatsModel;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.TunnelModel;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.ProxyMonitorCollector;
import com.ctrip.xpipe.redis.console.proxy.TunnelInfo;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class ProxyChainController extends AbstractConsoleController {

    @Autowired
    private ProxyService proxyService;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private RedisService redisService;

    @Autowired
    private DcService dcService;


    @RequestMapping(value = "/proxy/collectors/{dcName}", method = RequestMethod.GET)
    public List<ProxyPingStatsModel> getProxyMonitorCollectors(@PathVariable String dcName) {
        return proxyService.getProxyMonitorCollectors(dcName);
    }

    @RequestMapping(value = "/proxy/{proxyIp}/{dcName}", method = RequestMethod.GET)
    public List<TunnelModel> getTunnelModels(@PathVariable String dcName, @PathVariable String proxyIp) {
        logger.info("[getTunnelModels] {}, {}", dcName, proxyIp);
        List<TunnelInfo> tunnelInfos = proxyService.getProxyTunnels(dcName, proxyIp);
        if(tunnelInfos == null) {
            return Collections.emptyList();
        }
        List<TunnelModel> results = Lists.newArrayListWithCapacity(tunnelInfos.size());
        for(TunnelInfo info : tunnelInfos) {
            ProxyChain chain = proxyService.getProxyChain(info.getTunnelId());
            if(chain == null) {
                logger.warn("[tunnelId] {}, no chains", info.getTunnelId());
                continue;
            }
            results.add(new TunnelModel(info.getTunnelId(), chain.getBackupDc(), chain.getCluster(), chain.getShard(), info));
        }
        return results;
    }

    @RequestMapping(value = "/chain/{backupDcId}/{clusterId}/{shardId}", method = RequestMethod.GET)
    public ProxyChain getProxyChain(@PathVariable String backupDcId, @PathVariable String clusterId, @PathVariable String shardId) {
        return proxyService.getProxyChain(backupDcId, clusterId, shardId);
    }

    @RequestMapping(value = "/chain/{backupDcId}/{clusterId}", method = RequestMethod.GET)
    public List<ProxyChainModel> getProxyChains(@PathVariable String backupDcId, @PathVariable String clusterId) throws ResourceNotFoundException {
        List<ProxyChain> chains = proxyService.getProxyChains(backupDcId, clusterId);
        List<ProxyChainModel> result = Lists.newArrayListWithCapacity(chains.size());
        long activeDcId = clusterService.find(clusterId).getActivedcId();
        String activeDc = dcService.getDcName(activeDcId);
        for(ProxyChain chain : chains) {
            String shard = chain.getShard();

            List<RedisTbl> activeDcKeepers = redisService.findKeepersByDcClusterShard(activeDc, clusterId, shard);
            RedisTbl activeDcKeeper = filterout(activeDcKeepers, RedisTbl::isKeeperActive);

            List<RedisTbl> backupDcKeepers = redisService.findKeepersByDcClusterShard(chain.getBackupDc(), clusterId, shard);
            RedisTbl backupDcKeeper = filterout(backupDcKeepers, RedisTbl::isKeeperActive);

            List<RedisTbl> redises = redisService.findRedisesByDcClusterShard(activeDc, clusterId, shard);
            RedisTbl master = filterout(redises, RedisTbl::isMaster);

            result.add(new ProxyChainModel(chain, master, activeDcKeeper, backupDcKeeper));
        }
        return result;
    }

    private RedisTbl filterout(List<RedisTbl> redisTbls, Function<RedisTbl, Boolean> function) {
        for(RedisTbl redisTbl : redisTbls) {
            if(function.apply(redisTbl)) {
                return redisTbl;
            }
        }
        return null;
    }

}
