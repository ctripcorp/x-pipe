package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;
import com.ctrip.xpipe.redis.checker.spring.ConsoleDisableDbCondition;
import com.ctrip.xpipe.redis.checker.spring.DisableDbMode;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.dao.ProxyDao;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.model.consoleportal.ProxyInfoModel;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.ProxyChainCollector;
import com.ctrip.xpipe.redis.console.proxy.ProxyMonitorCollector;
import com.ctrip.xpipe.redis.console.proxy.ProxyMonitorCollectorManager;
import com.ctrip.xpipe.redis.console.proxy.impl.DefaultProxyMonitorCollector;
import com.ctrip.xpipe.redis.console.proxy.impl.DefaultTunnelInfo;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.ProxyService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.utils.ExceptionUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestOperations;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author chen.zhu
 * <p>
 * Jun 19, 2018
 */

@Service
@Conditional(ConsoleDisableDbCondition.class)
@DisableDbMode(false)
public class ProxyServiceImpl extends AbstractService implements ProxyService {

    @Autowired
    private ProxyDao proxyDao;

    @Autowired
    private DcService dcService;

    @Autowired
    private ProxyChainCollector proxyChainCollector;

    @Autowired
    private ProxyMonitorCollectorManager proxyMonitorCollectorManager;

    @Autowired
    private ShardService shardService;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    ConsoleConfig consoleConfig;

    private static final String CURRENT_DC = FoundationService.DEFAULT.getDataCenter();

    @Override
    public List<ProxyModel> getActiveProxies() {
        return convert(proxyDao.getActiveProxyTbls());
    }

    @Override
    public List<ProxyModel> getAllProxies() {
        return convert(proxyDao.getAllProxyTbls());
    }

    @Override
    public List<ProxyModel> getMonitorActiveProxiesByDc(String dcName) {
        return convert(proxyDao.getMonitorActiveProxyTblsByDc(dcName));
    }

    private List<ProxyModel> getAllMonitorActiveProxies() {
        return convert(proxyDao.getMonitorActiveProxyTbls());
    }

    private List<ProxyModel> convert(List<ProxyTbl> proxyTbls) {
        DcIdNameMapper mapper = new DcIdNameMapper.DefaultMapper(dcService);
        List<ProxyModel> clone = proxyTbls.stream().map(input -> {
            assert input != null;
            return ProxyModel.fromProxyTbl(input, mapper);
        }).collect(Collectors.toList());
        return Lists.newArrayList(clone);
    }

    @Override
    public Map<Long, String> proxyIdUriMap(){
        List<ProxyTbl> allProxies = proxyDao.getAllProxyTbls();
        Map<Long, String> proxyIdUriMap = new HashMap<>();

        allProxies.forEach((proxy -> {
            proxyIdUriMap.put(proxy.getId(), proxy.getUri());
        }));
        return proxyIdUriMap;
    }

    @Override
    public Map<String, Long> proxyUriIdMap(){
        List<ProxyTbl> allProxies = proxyDao.getAllProxyTbls();
        Map<String, Long> proxyUriIdMap = new HashMap<>();

        allProxies.forEach((proxy -> {
            proxyUriIdMap.put(proxy.getUri(), proxy.getId());
        }));
        return proxyUriIdMap;
    }

    @Override
    public void updateProxy(ProxyModel model) {
        DcIdNameMapper mapper = new DcIdNameMapper.OneTimeMapper(dcService);
        proxyDao.update(model.toProxyTbl(mapper));
    }

    @Override
    public void deleteProxy(long id) {
        proxyDao.delete(id);
    }

    @Override
    public void addProxy(ProxyModel model) {
        DcIdNameMapper mapper = new DcIdNameMapper.OneTimeMapper(dcService);
        proxyDao.insert(model.toProxyTbl(mapper));
    }

    @Override
    public List<ProxyTbl> getActiveProxyTbls() {
        return proxyDao.getActiveProxyTbls();
    }

    @Override
    public ProxyChain getProxyChain(String backupDcId, String clusterId, String shardId, String peerDcId) {
        return proxyChainCollector.getProxyChain(backupDcId, clusterId, shardId, peerDcId);
    }

    @Override
    public ProxyChain getProxyChain(String tunnelId) {
        return proxyChainCollector.getProxyChain(tunnelId);
    }

    @Override
    public List<DefaultTunnelInfo> getProxyTunnels(String dcName, String proxyIp) {
        if (StringUtil.isEmpty(dcName)) {
            return Collections.emptyList();
        }

        if (CURRENT_DC.equalsIgnoreCase(dcName)) {
            List<ProxyMonitorCollector> collectors = proxyMonitorCollectorManager.getProxyMonitorResults();
            for(ProxyMonitorCollector collector : collectors) {
                if(collector.getProxyInfo().getHostPort().getHost().equalsIgnoreCase(proxyIp)) {
                    return collector.getTunnelInfos();
                }
            }
            return Collections.emptyList();
        } else {
            String domain = getConsoleDomainByDc(dcName);
            ResponseEntity<List<DefaultTunnelInfo>> result = restTemplate.exchange(domain + "/api/proxy/tunnels/{proxyIp}/{dcName}",
                    HttpMethod.GET, null, new ParameterizedTypeReference<List<DefaultTunnelInfo>>() {}, proxyIp, dcName);
            return result == null ? Collections.emptyList() : Lists.newArrayList(result.getBody());
        }
    }

    @Override
    public Map<String, List<ProxyChain>> getProxyChains(String backupDcId, String clusterName) {
        List<ShardTbl> shards = shardService.findAllShardNamesByClusterName(clusterName);
        List<DcTbl> peerDcs = clusterService.getClusterRelatedDcs(clusterName);
        Map<String, List<ProxyChain>> proxyChains = Maps.newHashMap();
        for(ShardTbl shard : shards) {
            List<ProxyChain> peerChains = Lists.newArrayList();
            String shardId = shard.getShardName();
            for (DcTbl peerDcTbl : peerDcs) {
                String peerDc = peerDcTbl.getDcName();
                if (peerDc.equals(backupDcId)) continue;

                ProxyChain chain = proxyChainCollector.getProxyChain(backupDcId, clusterName, shardId, peerDc);
                if(chain != null) {
                    peerChains.add(chain);
                }
            }
            proxyChains.put(shardId, peerChains);
        }
        return proxyChains;
    }

    @Override
    public List<ProxyPingStatsModel> getProxyPingStatsModels(String dcName) {
        if(StringUtil.isEmpty(dcName)) {
            return Collections.emptyList();
        }
        if (CURRENT_DC.equalsIgnoreCase(dcName)) {
            List<ProxyMonitorCollector> collectors = proxyMonitorCollectorManager.getProxyMonitorResults();
            List<ProxyPingStatsModel> result = Lists.newArrayListWithCapacity(collectors.size());
            for (ProxyMonitorCollector collector : collectors) {
                result.add(new ProxyPingStatsModel(collector.getProxyInfo(), collector.getPingStatsResults()));
            }
            return result;
        } else {
            String domain = getConsoleDomainByDc(dcName);
            ResponseEntity<List<ProxyPingStatsModel>> result = restTemplate.exchange(domain + "/api/proxy/ping-stats/{dcName}",
                    HttpMethod.GET, null, new ParameterizedTypeReference<List<ProxyPingStatsModel>>() {}, dcName);
            return result == null ? Collections.emptyList() : Lists.newArrayList(result.getBody());
        }
    }

    private String getConsoleDomainByDc(String dcName) {
        return consoleConfig.getConsoleDomains().get(dcName.toUpperCase());
    }

    @Override
    public List<ProxyMonitorCollector> getAllProxyMonitorCollectors() {
        return proxyMonitorCollectorManager.getProxyMonitorResults();
    }

    @Override
    public List<ProxyInfoModel> getAllProxyInfo() {
        List<ProxyInfoModel> result = Collections.synchronizedList(new ArrayList<>());
        ParallelCommandChain commandChain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);

        consoleConfig.getConsoleDomains().forEach((dc , domain) -> {
            if (CURRENT_DC.equalsIgnoreCase(dc)) {
                updateAllProxyInfo(result, getAllProxyMonitorCollectors());
            } else {
                ProxyMonitorCollectorGetCommand command = new ProxyMonitorCollectorGetCommand(domain, restTemplate);
                command.future().addListener(commandFuture -> {
                    if (commandFuture.isSuccess() && commandFuture.get() != null) updateAllProxyInfo(result, commandFuture.get());
                });
                commandChain.add(command);
            }
        });

        try {
            commandChain.execute().get(10, TimeUnit.SECONDS);
        } catch (Throwable th) {
            logger.warn("[getAllProxyInfo] error:", th);
        }

        return result;
    }

    private void updateAllProxyInfo(List<ProxyInfoModel> result, List<? extends ProxyMonitorCollector> proxyMonitorCollectors) {
        for(ProxyMonitorCollector proxy : proxyMonitorCollectors) {
            ProxyModel model = proxy.getProxyInfo();
            int chainNum = getChainNumber(proxy);
            logger.debug("[getAllProxyInfo] get chainNum : {} of proxy {}", chainNum, proxy);
            result.add(new ProxyInfoModel(model.getHostPort().getHost(), model.getHostPort().getPort(), model.getDcName(), chainNum));
        }
    }

    public List<String> getActiveProxyUrisByDc(String dcName) {
        List<ProxyTbl> proxies = proxyDao.getActiveProxyTblsByDc(dcService.find(dcName).getId());
        List<String> proxyUri = new ArrayList<>();
        proxies.forEach((proxyTbl -> {
            proxyUri.add(proxyTbl.getUri());
        }));
        return proxyUri;
    }

    @Override
    public RetMessage deleteProxyChain(List<HostPort> proxies) {
        RetMessage message = null;
        for(HostPort hostPort : proxies) {
            message = notifyProxyNode(hostPort);
            if(message.getState() == RetMessage.SUCCESS_STATE) {
                return message;
            }
        }
        message = message == null ? RetMessage.createFailMessage("no host port received") : message;
        return message;
    }

    @Override
    public List<ProxyTunnelInfo> getAllProxyTunnels() {
        List<ProxyChain> chains = proxyChainCollector.getProxyChains();
        List<ProxyTunnelInfo> proxyTunnelInfos = new ArrayList<>();
        chains.forEach(chain -> proxyTunnelInfos.add(chain.buildProxyTunnelInfo()));
        return proxyTunnelInfos;
    }

    @Override
    public ProxyTunnelInfo getProxyTunnelInfo(String backupDcId, String clusterId, String shardId, String peerDcId) {
        ProxyChain chain = proxyChainCollector.getProxyChain(backupDcId, clusterId, shardId, peerDcId);
        if (null == chain) return null;
        return chain.buildProxyTunnelInfo();
    }

    @Override
    public void closeProxyTunnel(ProxyTunnelInfo proxyTunnelInfo) {
        deleteProxyChain(proxyTunnelInfo.getBackends());
    }

    private RetMessage notifyProxyNode(HostPort hostPort) {
        String message = null;
        try {
            logger.info("[notifyProxyNode]{} to close port {}", hostPort.getHost(), hostPort.getPort());
            restTemplate.delete(String.format("http://%s:8080/api/tunnel/local/port/%d", hostPort.getHost(), hostPort.getPort()));
        } catch (Exception e) {
            message = ExceptionUtils.getCause(e).getMessage();
            logger.error("[deleteProxyChain]", e);
            return RetMessage.createFailMessage(message);
        }
        return RetMessage.createSuccessMessage();
    }


    private int getChainNumber(ProxyMonitorCollector proxy) {
        List<DefaultTunnelInfo> tunnels = proxy.getTunnelInfos();
        int result = 0;
        for(DefaultTunnelInfo info : tunnels) {
            if(proxyChainCollector.getProxyChain(info.getTunnelId()) != null) {
                result ++;
            }
        }
        return result;
    }

    @VisibleForTesting
    public ProxyServiceImpl setShardService(ShardService service) {
        this.shardService = service;
        return this;
    }

    @VisibleForTesting
    public ProxyServiceImpl setProxyChainCollector(ProxyChainCollector proxyChainCollector) {
        this.proxyChainCollector = proxyChainCollector;
        return this;
    }

    @VisibleForTesting
    public ProxyServiceImpl setClusterService(ClusterService service) {
        this.clusterService = service;
        return this;
    }

    class ProxyMonitorCollectorGetCommand extends AbstractCommand<List<DefaultProxyMonitorCollector>> {

        private String domain;
        private RestOperations restTemplate;

        public ProxyMonitorCollectorGetCommand(String domain, RestOperations restTemplate) {
            this.domain = domain;
            this.restTemplate = restTemplate;
        }

        @Override
        public String getName() {
            return "getProxyMonitorCollector";
        }

        @Override
        protected void doExecute() throws Throwable {
            try {
                ResponseEntity<List<DefaultProxyMonitorCollector>> result =
                        restTemplate.exchange(domain + "/api/proxy/monitor-collectors", HttpMethod.GET, null,
                                new ParameterizedTypeReference<List<DefaultProxyMonitorCollector>>() {});
                future().setSuccess(result.getBody());
            } catch (Throwable th) {
                getLogger().error("get proxy monitor collector for domain:{} fail", domain, th);
                future().setFailure(th);
            }
        }

        @Override
        protected void doReset() {

        }
    }
}
