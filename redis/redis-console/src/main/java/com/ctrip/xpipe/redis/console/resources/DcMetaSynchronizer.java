package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderElector;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.MetaSynchronizer;
import com.ctrip.xpipe.redis.core.meta.comparator.DcSyncMetaComparator;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

@Component
public class DcMetaSynchronizer implements MetaSynchronizer {
    private static Logger logger = LoggerFactory.getLogger(DcMetaSynchronizer.class);
    static final String currentDcId = FoundationService.DEFAULT.getDataCenter();
    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("XPipe-Meta-Sync"));
    private OuterClientService outerClientService = OuterClientService.DEFAULT;
    private Pattern filterClusterPattern;

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private RedisService redisService;

    @Autowired
    private ShardService shardService;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private DcService dcService;

    @Autowired
    private OrganizationService organizationService;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired(required = false)
    private ConsoleLeaderElector consoleLeaderElector;

    @Autowired
    private SentinelBalanceService sentinelBalanceService;

    private Map<Long, OrganizationTbl> organizations = new HashMap<>();

    @PostConstruct
    public void start() {
        scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                if (consoleLeaderElector != null && consoleLeaderElector.amILeader()) {
                    sync();
                }
            }
        }, consoleConfig.getOuterClientSyncInterval(), consoleConfig.getOuterClientSyncInterval(), TimeUnit.MILLISECONDS);
    }

    public void sync() {
        TransactionMonitor transaction = TransactionMonitor.DEFAULT;
        transaction.logTransactionSwallowException("meta.sync", currentDcId, new Task() {

            DcMeta future;
            DcMeta current;
            Pair<DcMeta, Set<String>> outerDcMeta = new Pair<>();

            @Override
            public void go() throws Exception {
                try {
                    buildFilterPattern();
                    refreshOrganizationsCache();
                    outerDcMeta = extractOuterDcMetaWithInterestedTypes(getDcMetaFromOutClient(currentDcId));
                    future = outerDcMeta.getKey();
                    current = extractLocalDcMetaWithInterestedTypes(metaCache.getXpipeMeta(), outerDcMeta.getValue());
                    DcSyncMetaComparator dcMetaComparator = new DcSyncMetaComparator(current, future);
                    dcMetaComparator.compare();
                    new ClusterMetaSynchronizer(dcMetaComparator.getAdded(), dcMetaComparator.getRemoved(), dcMetaComparator.getMofified(), dcService, clusterService, shardService, redisService, organizationService, sentinelBalanceService, consoleConfig).sync();
                } catch (Throwable e) {
                    logger.error("[DcMetaSynchronizer][sync]", e);
                }
            }

            @Override
            public Map<String, Object> getData() {
                Map<String, Object> transactionData = new HashMap<>();
                transactionData.put("current", current);
                transactionData.put("future", future);
                transactionData.put("filtered", outerDcMeta.getValue());
                return transactionData;
            }
        });
    }

    void buildFilterPattern() {
        String filterPattern = consoleConfig.filterOuterClusters();
        if (!Strings.isNullOrEmpty(filterPattern))
            filterClusterPattern = Pattern.compile(filterPattern);
    }

    void refreshOrganizationsCache() {
        try {
            List<OrganizationTbl> organizationTbls = organizationService.getAllOrganizations();
            Map<Long, OrganizationTbl> future = new HashMap<>();
            for (OrganizationTbl organizationTbl : organizationTbls) {
                future.put(organizationTbl.getOrgId(), organizationTbl);
            }
            if (future.isEmpty()) {
                logger.warn("[DcMetaSynchronizer][refreshAllOrganizations]{}", "organizationTbls empty");
            } else {
                organizations = future;
            }
        } catch (Exception e) {
            logger.error("[DcMetaSynchronizer][refreshAllOrganizations]", e);
        }
    }

    OuterClientService.DcMeta getDcMetaFromOutClient(String dc) throws Exception {
        return outerClientService.getOutClientDcMeta(dc);
    }

    Pair<DcMeta, Set<String>> extractOuterDcMetaWithInterestedTypes(OuterClientService.DcMeta outerDcMeta) {
        DcMeta dcMeta = new DcMeta(outerDcMeta.getDcName());
        Map<String, OuterClientService.ClusterMeta> outerClusterMetas = outerDcMeta.getClusters();
        Set<String> filterClusters = new HashSet<>();
        for (OuterClientService.ClusterMeta outerClusterMeta : outerClusterMetas.values()) {
            try {
                if (shouldFilterOuterCluster(outerClusterMeta)) {
                    filterClusters.add(outerClusterMeta.getName());
                    continue;
                }
                ClusterMeta clusterMeta = outerClusterToInner(outerClusterMeta);
                if (clusterMeta != null)
                    dcMeta.addCluster(clusterMeta);

            } catch (Throwable e) {
                logger.error("[extractOuterDcMetaWithInterestedTypes]: {}", outerClusterMeta.getName(), e);
            }
        }
        return new Pair<>(dcMeta, filterClusters);
    }

    DcMeta extractLocalDcMetaWithInterestedTypes(XpipeMeta xpipeMeta, Set<String> filteredOuterClusters) {
        DcMeta dcMeta = xpipeMeta.findDc(currentDcId);
        if (dcMeta == null)
            return new DcMeta(currentDcId);

        DcMeta dcMetaWithInterestedClusters = new DcMeta(dcMeta.getId());
        List<ClusterMeta> interestedClusters = new ArrayList<>();
        for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
            try {
                if (!shouldFilterInnerCluster(clusterMeta, filteredOuterClusters)) {
                    interestedClusters.add(newClusterMeta(clusterMeta));
                }
            } catch (Throwable e) {
                logger.error("[extractLocalDcMetaWithInterestedTypes]: {}", clusterMeta.getId(), e);
            }
        }

        interestedClusters.forEach(clusterMeta -> {
            dcMetaWithInterestedClusters.addCluster(clusterMeta);
        });
        return dcMetaWithInterestedClusters;
    }

    ClusterMeta outerClusterToInner(OuterClientService.ClusterMeta outer) {
        try {
            ClusterMeta clusterMeta = new ClusterMeta(outer.getName());
            clusterMeta.setType(outer.getClusterType().innerType().name());
            if (ClusterType.lookup(clusterMeta.getType()).supportSingleActiveDC()) {
                clusterMeta.setActiveDc(currentDcId);
            } else {
                clusterMeta.setDcs(currentDcId);
            }
            clusterMeta.setAdminEmails(outer.getOwnerEmails());
            OrganizationTbl organization = organizations.get((long) outer.getOrgId());
            if (organization != null)
                clusterMeta.setOrgId(organization.getId().intValue());
            else
                clusterMeta.setOrgId(0);
            Map<String, OuterClientService.GroupMeta> groups = outer.getGroups();
            for (String groupName : groups.keySet()) {
                clusterMeta.addShard(outerShardToInner(groups.get(groupName)).setParent(clusterMeta));
            }
            return clusterMeta;
        } catch (Exception e) {
            logger.error("[DcMetaSynchronizer]outerClusterToInner {}", outer.getName(), e);
            return null;
        }
    }

    ClusterMeta newClusterMeta(ClusterMeta origin) {
        ClusterMeta clusterMeta = new ClusterMeta(origin.getId()).setType(origin.getType()).setAdminEmails(origin.getAdminEmails()).setOrgId(origin.getOrgId()).setActiveDc(origin.getActiveDc());
        Map<String, ShardMeta> groups = origin.getShards();
        for (ShardMeta shard : groups.values()) {
            clusterMeta.addShard(newShardMeta(shard).setParent(clusterMeta));
        }
        return clusterMeta;
    }

    ShardMeta outerShardToInner(OuterClientService.GroupMeta outer) {
        ShardMeta shardMeta = new ShardMeta(outer.getGroupName());
        shardMeta.setSentinelMonitorName(SentinelUtil.getSentinelMonitorName(outer.getClusterName(), outer.getGroupName(), currentDcId));
        List<OuterClientService.RedisMeta> redisMetaList = outer.getRedises();
        for (OuterClientService.RedisMeta redisMeta : redisMetaList) {
            shardMeta.addRedis(outerRedisToInner(redisMeta).setParent(shardMeta));
        }
        return shardMeta;
    }

    ShardMeta newShardMeta(ShardMeta origin) {
        ShardMeta shardMeta = new ShardMeta(origin.getId());
        shardMeta.setSentinelMonitorName(origin.getSentinelMonitorName());
        List<RedisMeta> redisMetaList = origin.getRedises();
        for (RedisMeta redisMeta : redisMetaList) {
            shardMeta.addRedis(newRedis(redisMeta).setParent(shardMeta));
        }
        return shardMeta;
    }

    RedisMeta outerRedisToInner(OuterClientService.RedisMeta outer) {
        RedisMeta redisMeta = new RedisMeta();
        redisMeta.setIp(outer.getHost());
        redisMeta.setPort(outer.getPort());
        if (outer.isMaster())
            redisMeta.setMaster("");
        else
            redisMeta.setMaster(XPipeConsoleConstant.DEFAULT_ADDRESS);
        return redisMeta;
    }

    RedisMeta newRedis(RedisMeta origin) {
        RedisMeta redisMeta = new RedisMeta();
        redisMeta.setIp(origin.getIp());
        redisMeta.setPort(origin.getPort());
        redisMeta.setMaster(origin.getMaster());
        return redisMeta;
    }

    boolean shouldFilterOuterCluster(OuterClientService.ClusterMeta clusterMeta) {
        return notInterestedTypes(clusterMeta.getClusterType().innerType().name()) || isOperating(clusterMeta) || nameMatchFilterPattern(clusterMeta.getName());
    }

    boolean shouldFilterInnerCluster(ClusterMeta clusterMeta, Set<String> filteredOuterClusters) {
        return notInterestedTypes(clusterMeta.getType()) || filteredOuterClusters.contains(clusterMeta.getId());
    }

    boolean nameMatchFilterPattern(String clusterName) {
        return filterClusterPattern != null && filterClusterPattern.matcher(clusterName).find();
    }

    boolean isOperating(OuterClientService.ClusterMeta clusterMeta) {
        return clusterMeta.isOperating();
    }

    boolean notInterestedTypes(String clusterType) {
        return !consoleConfig.getOuterClusterTypes().contains(clusterType.toUpperCase());
    }

}
