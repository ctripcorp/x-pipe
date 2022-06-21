package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.DefaultRetryCommandFactory;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.command.RetryCommandFactory;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.meta.*;
import com.ctrip.xpipe.redis.console.service.vo.DcMetaBuilder;
import com.ctrip.xpipe.redis.core.entity.AzMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.retry.RetryDelay;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

/**
 * @author chen.zhu
 * <p>
 * Apr 02, 2018
 */
@Service
public class AdvancedDcMetaService implements DcMetaService {

    private static final Logger logger = LoggerFactory.getLogger(AdvancedDcMetaService.class);

    @Autowired
    private DcService dcService;

    @Autowired
    private ZoneService zoneService;

    @Autowired
    private DcClusterShardService dcClusterShardService;

    @Autowired
    private SentinelGroupService sentinelService;

    @Autowired
    private KeeperContainerService keeperContainerService;

    @Autowired
    private AzService azService;

    @Autowired
    private SentinelMetaService sentinelMetaService;

    @Autowired
    private KeepercontainerMetaService keepercontainerMetaService;

    @Autowired
    private RedisMetaService redisMetaService;

    @Autowired
    private DcClusterService dcClusterService;

    @Autowired
    private ClusterMetaService clusterMetaService;

    @Autowired
    private RouteService routeService;

    @Autowired
    private ProxyService proxyService;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Resource(name=SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    private ExecutorService executors;

    private RetryCommandFactory factory;

    @PostConstruct
    public void initService() {
        int corePoolSize = Math.min(Integer.parseInt(System.getProperty("maximum.pool.size", "20")), OsUtils.getCpuCount() * 5);
        executors = DefaultExecutorFactory.createAllowCoreTimeout("AdvancedDcMetaService", corePoolSize).createExecutorService();
        int retryTimeoutMilli = 3000, retryDelayMilli = 5;
        factory = new DefaultRetryCommandFactory(new RetryDelay(retryDelayMilli), retryTimeoutMilli, scheduled);
    }

    @Override
    public DcMeta getDcMeta(String dcName) {
        return getDcMeta(dcName, consoleConfig.getOwnClusterType());
    }

    @Override
    public DcMeta getDcMeta(String dcName, Set<String> allowTypes) {
        DcTbl dcTbl = dcService.find(dcName);
        ZoneTbl zoneTbl = zoneService.findById(dcTbl.getZoneId());

        DcMeta dcMeta = new DcMeta().setId(dcName).setLastModifiedTime(dcTbl.getDcLastModifiedTime()).setZone(zoneTbl.getZoneName());

        ParallelCommandChain chain = new ParallelCommandChain(executors, false);
        chain.add(retry3TimesUntilSuccess(new GetAllSentinelCommand(dcMeta)));
        chain.add(retry3TimesUntilSuccess(new GetAllKeeperContainerCommand(dcMeta)));
        chain.add(retry3TimesUntilSuccess(new GetAllRouteCommand(dcMeta)));
        chain.add(retry3TimesUntilSuccess(new GetAllAavailableZoneCommand(dcMeta)));

        DcMetaBuilder builder = new DcMetaBuilder(dcMeta, dcTbl.getId(), allowTypes, executors, redisMetaService, dcClusterService,
                clusterMetaService, dcClusterShardService, dcService, factory, consoleConfig);
        chain.add(retry3TimesUntilSuccess(builder));

        try {
            chain.execute().get();
        } catch (Exception e) {
            logger.error("[queryDcMeta] ", e);
        }

        return dcMeta;
    }

    @VisibleForTesting
    protected <T> Command<T> retry3TimesUntilSuccess(Command<T> command) {
        return factory.createRetryCommand(command);
    }

    class GetAllSentinelCommand extends AbstractCommand<Void> {

        private DcMeta dcMeta;

        public GetAllSentinelCommand(DcMeta dcMeta) {
            this.dcMeta = dcMeta;
        }

        @Override
        protected void doExecute() throws Exception {
            try {
                List<SentinelGroupModel> sentinels = sentinelService.findAllByDcName(dcMeta.getId());
                sentinels.forEach(sentinel -> dcMeta
                        .addSentinel(sentinelMetaService.encodeSetinelMeta(sentinel, dcMeta)));
                future().setSuccess();
            } catch (Throwable th) {
                future().setFailure(th);
            }
        }

        @Override
        protected void doReset() {
            dcMeta.getSentinels().clear();
        }

        @Override
        public String getName() {
            return this.getClass().getSimpleName();
        }
    }

    class GetAllAavailableZoneCommand extends AbstractCommand<Void> {
        private DcMeta dcMeta;

        public GetAllAavailableZoneCommand(DcMeta dcMeta) {
            this.dcMeta = dcMeta;
        }

        @Override
        protected void doExecute() throws Throwable {
            try {
                List<AzTbl> azTbls = azService.getDcActiveAvailableZoneTbls(dcMeta.getId());
                if(azTbls != null && !azTbls.isEmpty()) {
                    azTbls.forEach(aztbl -> {
                        dcMeta.addAz(encodeAzMeta(aztbl, dcMeta));
                    });
                }
                future().setSuccess();
            } catch (Throwable th) {
                future().setFailure(th);
            }
        }

        private  AzMeta encodeAzMeta(AzTbl azTbl, DcMeta dcMeta) {
            AzMeta azMeta = new AzMeta();

            if(null != azTbl) {
                azMeta.setId(azTbl.getAzName());
                azMeta.setActive(azTbl.isActive());
                azMeta.setParent(dcMeta);
            }

            return azMeta;
        }

        @Override
        protected void doReset() {
            dcMeta.getAzs().clear();
        }

        @Override
        public String getName() {
            return this.getClass().getSimpleName();
        }
    }

    class GetAllKeeperContainerCommand extends AbstractCommand<Void> {

        private DcMeta dcMeta;

        public GetAllKeeperContainerCommand(DcMeta dcMeta) {
            this.dcMeta = dcMeta;
        }

        @Override
        protected void doExecute() throws Exception {
            try {
                List<KeepercontainerTbl> keepercontainers = keeperContainerService.findAllByDcName(dcMeta.getId());
                keepercontainers.forEach(keeperContainer -> dcMeta.addKeeperContainer(
                        keepercontainerMetaService.encodeKeepercontainerMeta(keeperContainer, dcMeta)));
                future().setSuccess();
            } catch (Throwable th) {
                future().setFailure(th);
            }
        }

        @Override
        protected void doReset() {
            dcMeta.getKeeperContainers().clear();
        }

        @Override
        public String getName() {
            return this.getClass().getSimpleName();
        }
    }

    class GetAllRouteCommand extends AbstractCommand<Void> {

        private DcMeta dcMeta;

        public GetAllRouteCommand(DcMeta dcMeta) {
            this.dcMeta = dcMeta;
        }

        @Override
        protected void doExecute() throws Exception {
            try {
                List<RouteTbl> routes = routeService.getActiveRouteTbls();
                List<ProxyTbl> proxies = proxyService.getActiveProxyTbls();
                List<RouteMeta> routeMetas = combineRouteInfo(routes, proxies, dcMeta);
                routeMetas.forEach((routeMeta)->dcMeta.addRoute(routeMeta));
                future().setSuccess();
            } catch (Throwable th) {
                future().setFailure(th);
            }
        }

        @Override
        protected void doReset() {
            dcMeta.getRoutes().clear();
        }

        @Override
        public String getName() {
            return this.getClass().getSimpleName();
        }
    }

    @VisibleForTesting
    protected List<RouteMeta> combineRouteInfo(List<RouteTbl> routes, List<ProxyTbl> proxies, DcMeta dcMeta) {
        List<DcTbl> dcTbls = dcService.findAllDcBasic();
        Map<Long, ProxyTbl> proxyTblMap = convertToMap(proxies);
        List<RouteMeta> result = Lists.newArrayListWithCapacity(routes.size());
        for(RouteTbl route : routes) {
            if(!dcMeta.getId().equals(getDcName(route.getSrcDcId(), dcTbls))) {
                continue;
            }
            RouteMeta routeMeta = new RouteMeta();
            routeMeta.setId(route.getId()).setOrgId((int) route.getRouteOrgId()).setTag(route.getTag());
            routeMeta.setSrcDc(getDcName(route.getSrcDcId(), dcTbls)).setDstDc(getDcName(route.getDstDcId(), dcTbls));
            routeMeta.setRouteInfo(getRouteInfo(route, proxyTblMap));
            routeMeta.setIsPublic(route.isIsPublic());
            result.add(routeMeta);
        }
        return result;
    }

    private String getDcName(long id, List<DcTbl> dcTbls) {
        for(DcTbl dc : dcTbls) {
            if(dc.getId() == id) {
                return dc.getDcName();
            }
        }
        return null;
    }

    @VisibleForTesting
    protected String getRouteInfo(RouteTbl route, Map<Long, ProxyTbl> proxyTblMap) {
        StringBuilder sb = new StringBuilder();
        fetchRouteInfo(route.getSrcProxyIds(), proxyTblMap, sb);
        fetchRouteInfo(route.getOptionalProxyIds(), proxyTblMap, sb);
        fetchRouteInfo(route.getDstProxyIds(), proxyTblMap, sb);
        if(sb.length() > 0 && sb.charAt(sb.length() - 1) == ' ') {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    @VisibleForTesting
    protected void fetchRouteInfo(String ids, Map<Long, ProxyTbl> proxyTblMap, StringBuilder sb) {
        if(ids == null || ids.isEmpty()) {
            return;
        }
        String splitter = "\\s*,\\s*";
        String[] srcIds = StringUtil.splitRemoveEmpty(splitter, ids);
        for(String srcId : srcIds) {
            long id = Long.parseLong(srcId);
            ProxyTbl proxy = proxyTblMap.get(id);
            if(proxy != null) {
                sb.append(proxy.getUri()).append(",");
            }
        }
        if(sb.length() > 0 && sb.charAt(sb.length()-1) == ',') {
            sb.deleteCharAt(sb.length() - 1);
            sb.append(" ");
        }
    }

    private Map<Long, ProxyTbl> convertToMap(List<ProxyTbl> proxies) {
        Map<Long, ProxyTbl> map = Maps.newHashMap();
        for(ProxyTbl proxy : proxies) {
            map.put(proxy.getId(), proxy);
        }
        return map;
    }

    /**-----------------------Visible for Test-----------------------------------------*/
    public AdvancedDcMetaService setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
        return this;
    }

    public AdvancedDcMetaService setExecutors(ExecutorService executors) {
        this.executors = executors;
        return this;
    }

    public AdvancedDcMetaService setFactory(RetryCommandFactory factory) {
        this.factory = factory;
        return this;
    }
}
