package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardPeer;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.ProxyChainAnalyzer;
import com.ctrip.xpipe.redis.console.proxy.ProxyChainCollector;
import com.ctrip.xpipe.redis.console.reporter.DefaultHttpService;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;
import static java.lang.String.format;

@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class DefaultProxyChainCollector extends AbstractStartStoppable implements ProxyChainCollector {

    @Resource(name = SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Autowired
    private ProxyChainAnalyzer proxyChainAnalyzer;

    @Autowired
    private ConsoleConfig consoleConfig;

    private DefaultHttpService httpService = new DefaultHttpService();

    private ScheduledFuture future;

    private AtomicBoolean taskTrigger = new AtomicBoolean(false);

    private String currentDc = FoundationService.DEFAULT.getDataCenter();

    private Map<DcClusterShardPeer, ProxyChain> shardProxyChainMap = Maps.newConcurrentMap();

    private volatile Map<String, DcClusterShardPeer> tunnelClusterShardMap = Maps.newConcurrentMap();

    private Map<String, Map<DcClusterShardPeer, ProxyChain>> dcProxyChainMap = Maps.newConcurrentMap();

    @Override
    public void isleader() {
        taskTrigger.set(true);
        try {
            logger.info("isLeader {}", getClass().getSimpleName());
            start();
        }catch (Throwable th) {
            logger.error("[notLeader]", th);
        }
    }

    @Override
    public void notLeader() {
        taskTrigger.set(false);
        try {
            logger.info("isNotLeader {}", getClass().getSimpleName());
            stop();
        }catch (Throwable th) {
            logger.error("[notLeader]", th);
        }
    }

    @Override
    public ProxyChain getProxyChain(String backupDcId, String clusterId, String shardId, String peerDcId) {
        return shardProxyChainMap.get(new DcClusterShardPeer(backupDcId, clusterId, shardId, peerDcId));
    }

    @Override
    public ProxyChain getProxyChain(String tunnelId) {
        if(tunnelClusterShardMap.containsKey(tunnelId)) {
            return shardProxyChainMap.get(tunnelClusterShardMap.get(tunnelId));
        }
        return null;
    }

    @Override
    public List<ProxyChain> getProxyChains() {
        return Lists.newArrayList(shardProxyChainMap.values());
    }

    @Override
    public Map<String, Map<DcClusterShardPeer, ProxyChain>> getDcProxyChainMap() {
        return dcProxyChainMap;
    }

    @Override
    protected void doStart() throws Exception {
        scheduled.scheduleWithFixedDelay(() -> {
            if (!taskTrigger.get()) {
                return;
            }
            logger.info("proxy chain collector started");
            getAllDcProxyChains();
        }, getStartTime(), getPeriodic(), TimeUnit.MILLISECONDS);
    }

    private void getAllDcProxyChains() {
        ParallelCommandChain commandChain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);
        consoleConfig.getConsoleDomains().forEach((dc, domain)->{
            logger.info("begin to get proxy chain from dc {} {}", dc, domain);
            if(currentDc.equalsIgnoreCase(dc)) {
                dcProxyChainMap.put(dc, proxyChainAnalyzer.getClusterShardChainMap());
            } else {
                Command command = new ProxyChainGetCommand(dc, domain , httpService.getRestTemplate());
                command.future().addListener(commandFuture -> {
                    logger.info("success to get proxy chain from dc {} {} {}", dc, domain, commandFuture.isSuccess());
                    if (commandFuture.isSuccess()) dcProxyChainMap.put(dc, (Map<DcClusterShardPeer, ProxyChain>) commandFuture.get());
                });
                commandChain.add(command);
            }
        });
        commandChain.execute().addListener(commandFuture -> {
            logger.info("start to aggregate proxy chain");
            updateShardProxyChainMap();
        });
    }

    @VisibleForTesting
    void updateShardProxyChainMap() {
        Map<DcClusterShardPeer, ProxyChain> tempShardProxyChain = Maps.newConcurrentMap();
        Map<String, DcClusterShardPeer> tempTunnelClusterShardMap = Maps.newConcurrentMap();
        logger.info("update proxy chains {}", dcProxyChainMap);
        dcProxyChainMap.forEach((dc, proxyChainMap) -> {
            proxyChainMap.forEach((clusterShard, proxyChain) -> {
                if (tempShardProxyChain.containsKey(clusterShard)) {
                    tempShardProxyChain.get(clusterShard).getTunnelInfos().add(proxyChain.getTunnelInfos().get(0));
                } else {
                    tempShardProxyChain.put(clusterShard, proxyChain);
                }
                tempTunnelClusterShardMap.put(proxyChain.getTunnelInfos().get(0).getTunnelId(), clusterShard);
            });
        });
        synchronized (DefaultProxyChainCollector.this) {
            tunnelClusterShardMap = tempTunnelClusterShardMap;
            shardProxyChainMap = tempShardProxyChain;
            logger.info("after update {}", shardProxyChainMap);
        }
    }

    @VisibleForTesting
    DefaultProxyChainCollector setDcProxyChainMap(Map<String, Map<DcClusterShardPeer, ProxyChain>> dcProxyChainMap) {
        this.dcProxyChainMap = dcProxyChainMap;
        return this;
    }

    @Override
    public Map<DcClusterShardPeer, ProxyChain> getShardProxyChainMap() {
        return shardProxyChainMap;
    }

    @VisibleForTesting
    Map<String, DcClusterShardPeer> getTunnelClusterShardMap() {
        return tunnelClusterShardMap;
    }

    protected int getStartTime() {
        return 2 * 1000;
    }

    protected int getPeriodic() {
        return 1000;
    }

    @Override
    protected void doStop() throws Exception {
        if(future != null) {
            future.cancel(true);
            future = null;
        }
        shardProxyChainMap.clear();
        tunnelClusterShardMap.clear();
        dcProxyChainMap.clear();
    }

    @Override
    public Map<DcClusterShardPeer, ProxyChain> getAllProxyChains() {
        return shardProxyChainMap;
    }

    class  ProxyChainGetCommand extends AbstractCommand<Map<DcClusterShardPeer, DefaultProxyChain>> {
        private String dcName;
        private String domain;
        private RestOperations restTemplate;

        public ProxyChainGetCommand(String dcName, String domain, RestOperations restTemplate) {
            this.dcName = dcName;
            this.domain = domain;
            this.restTemplate = restTemplate;
        }

        @Override
        public String getName() {
            return "getProxyChain";
        }

        @Override
        protected void doExecute() throws Throwable {
            try {
                ResponseEntity<Map<DcClusterShardPeer, DefaultProxyChain>> result =
                        restTemplate.exchange(format("%s/api/proxy/chains/{dcName}",
                        domain), HttpMethod.GET, null,  new ParameterizedTypeReference<Map<DcClusterShardPeer, DefaultProxyChain>>(){}, dcName);
                future().setSuccess(result.getBody());
            } catch (Throwable th) {
                getLogger().error("get proxy chain for dc:{} fail", dcName, th);
                future().setFailure(th);
            }
        }

        @Override
        protected void doReset() {

        }
    }
}
