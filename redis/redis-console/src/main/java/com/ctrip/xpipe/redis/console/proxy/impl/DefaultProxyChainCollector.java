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
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;

@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class DefaultProxyChainCollector extends AbstractStartStoppable implements ProxyChainCollector {

    private ProxyChainAnalyzer proxyChainAnalyzer;

    private ConsoleConfig consoleConfig;

    private DefaultHttpService httpService = new DefaultHttpService();

    private DynamicDelayPeriodTask collectTask;

    private ScheduledExecutorService scheduled;

    private AtomicBoolean taskTrigger = new AtomicBoolean(false);

    private String currentDc = FoundationService.DEFAULT.getDataCenter();

    private volatile Map<DcClusterShardPeer, ProxyChain> shardProxyChainMap = Maps.newConcurrentMap();

    private volatile Map<String, DcClusterShardPeer> tunnelClusterShardMap = Maps.newConcurrentMap();

    private Map<String, Map<DcClusterShardPeer, ProxyChain>> dcProxyChainMap = Maps.newConcurrentMap();

    @Autowired
    public DefaultProxyChainCollector(ProxyChainAnalyzer proxyChainAnalyzer, ConsoleConfig consoleConfig) {
        this.proxyChainAnalyzer = proxyChainAnalyzer;
        this.consoleConfig = consoleConfig;
        this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create(getClass().getSimpleName()));
        this.collectTask = new DynamicDelayPeriodTask(getClass().getSimpleName(), this::fetchAllDcProxyChains,
                consoleConfig::getProxyInfoCollectInterval, scheduled);
    }

    @Override
    public void isleader() {
        if(consoleConfig.disableDb()) {
            return;
        }
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
    public Map<String, DcClusterShardPeer> getTunnelClusterShardMap() {
        return tunnelClusterShardMap;
    }

    @Override
    protected void doStart() throws Exception {
        collectTask.start();
    }

    @Override
    protected void doStop() throws Exception {
        collectTask.stop();
        clear();
    }

    protected void fetchAllDcProxyChains() {
        if (!taskTrigger.get()) {
            return;
        }

        ParallelCommandChain commandChain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);
        consoleConfig.getConsoleDomains().forEach((dc, domain)->{
            logger.debug("begin to get proxy chain from dc {} {}", dc, domain);
            if(currentDc.equalsIgnoreCase(dc)) {
                dcProxyChainMap.put(dc, proxyChainAnalyzer.getClusterShardChainMap());
            } else {
                Command command = new ProxyChainGetCommand(dc, domain , httpService.getRestTemplate());
                command.future().addListener(commandFuture -> {
                    logger.debug("success to get proxy chain from dc {} {} {}", dc, domain, commandFuture.isSuccess());
                    if (commandFuture.isSuccess() && commandFuture.get() != null)
                        dcProxyChainMap.put(dc, (Map<DcClusterShardPeer, ProxyChain>) commandFuture.get());
                });
                commandChain.add(command);
            }
        });
        commandChain.execute().addListener(commandFuture -> {
            updateShardProxyChainMap();
        });
    }

    @VisibleForTesting
    void updateShardProxyChainMap() {
        Map<DcClusterShardPeer, ProxyChain> tempShardProxyChain = Maps.newConcurrentMap();
        Map<String, DcClusterShardPeer> tempTunnelClusterShardMap = Maps.newConcurrentMap();

        dcProxyChainMap.forEach((dc, proxyChainMap) -> {
            proxyChainMap.forEach((clusterShard, proxyChain) -> {
                DefaultTunnelInfo tunnel = proxyChain.getTunnelInfos().get(0);
                if (!tempShardProxyChain.containsKey(clusterShard)) {
                    tempShardProxyChain.put(clusterShard, new DefaultProxyChain(proxyChain.getBackupDcId(),
                            proxyChain.getClusterId(), proxyChain.getShardId(), proxyChain.getPeerDcId(), new ArrayList<>()));
                }
                tempShardProxyChain.get(clusterShard).getTunnelInfos().add(tunnel);
                tempTunnelClusterShardMap.put(tunnel.getTunnelId(), clusterShard);
            });
        });
        synchronized (DefaultProxyChainCollector.this) {
            if (taskTrigger.get()) {
                tunnelClusterShardMap = tempTunnelClusterShardMap;
                shardProxyChainMap = tempShardProxyChain;
            } else {
                clear();
            }
        }
    }

    @VisibleForTesting
    protected void setTaskTrigger(boolean trigger) {
        this.taskTrigger.set(trigger);
    }

    @VisibleForTesting
    DefaultProxyChainCollector setHttpService(DefaultHttpService httpService) {
        this.httpService = httpService;
        return this;
    }

    @Override
    public Map<DcClusterShardPeer, ProxyChain> getShardProxyChainMap() {
        return shardProxyChainMap;
    }

    protected void clear() {
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
                        restTemplate.exchange(format("%s/api/proxy/chains/{dcName}", domain), HttpMethod.GET, null,
                                new ParameterizedTypeReference<Map<DcClusterShardPeer, DefaultProxyChain>>(){}, dcName);
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
