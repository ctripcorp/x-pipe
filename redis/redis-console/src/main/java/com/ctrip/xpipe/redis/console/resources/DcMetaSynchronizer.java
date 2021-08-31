package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderElector;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.MetaSynchronizer;
import com.ctrip.xpipe.redis.core.meta.comparator.DcMetaComparator;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class DcMetaSynchronizer implements MetaSynchronizer {
    private static Logger logger = LoggerFactory.getLogger(DcMetaSynchronizer.class);
    static final String currentDcId = FoundationService.DEFAULT.getDataCenter();
    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("XPipe-Meta-Sync"));
    private OuterClientService outerClientService = OuterClientService.DEFAULT;

    @Autowired
    private MetaCache metaCache;

    @Autowired
    public RedisService redisService;

    @Autowired
    public ShardService shardService;

    @Autowired
    public ClusterService clusterService;

    @Autowired
    public DcService dcService;

    @Autowired
    public OrganizationService organizationService;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired(required = false)
    private ConsoleLeaderElector consoleLeaderElector;

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
        try {
            refreshAllOrganizations();
            DcMeta current = extractLocalDcMetaWithInterestedTypes(metaCache.getXpipeMeta(), consoleConfig.getOuterClusterTypes());
            DcMeta future = extractOuterDcMetaWithInterestedTypes(getDcMetaFromOutClient(currentDcId), consoleConfig.getOuterClusterTypes());
            DcMetaComparator dcMetaComparator = new DcMetaComparator(current, future);
            dcMetaComparator.compare();
            logger.info("[DcMetaSynchronizer][sync]added:{}, removed:{}, modified:{}", dcMetaComparator.getAdded(), dcMetaComparator.getRemoved(), dcMetaComparator.getMofified());
            new ClusterMetaSynchronizer(dcMetaComparator.getAdded(), dcMetaComparator.getRemoved(), dcMetaComparator.getMofified(), dcService, clusterService, shardService, redisService, organizationService).sync();
        } catch (Exception e) {
            logger.error("[DcMetaSynchronizer][sync]", e);
        }
    }

    void refreshAllOrganizations() {
        try {
            List<OrganizationTbl> organizationTbls = organizationService.getAllOrganizations();
            for (OrganizationTbl organizationTbl : organizationTbls) {
                organizations.put(organizationTbl.getOrgId(), organizationTbl);
            }
        } catch (Exception e) {
            logger.error("[DcMetaSynchronizer][refreshAllOrganizations]", e);
        }
    }

    OuterClientService.DcMeta getDcMetaFromOutClient(String dc) throws Exception {
        return outerClientService.getOutClientDcMeta(dc);
    }

    DcMeta extractOuterDcMetaWithInterestedTypes(OuterClientService.DcMeta outerDcMeta, Set<String> interestedTypes) {
        DcMeta dcMeta = new DcMeta(outerDcMeta.getDcName());
        Map<String, OuterClientService.ClusterMeta> outerClusterMetas = outerDcMeta.getClusters();
        outerClusterMetas.values().forEach(outerClusterMeta -> {
            if (interestedTypes.contains(innerClusterType(outerClusterMeta.getClusterType())))
                dcMeta.addCluster(outerClusterToInner(outerClusterMeta));
        });
        return dcMeta;
    }

    DcMeta extractLocalDcMetaWithInterestedTypes(XpipeMeta xpipeMeta, Set<String> interestedTypes) {
        DcMeta dcMeta = xpipeMeta.findDc(currentDcId);
        List<ClusterMeta> interestedClusters = new ArrayList<>();
        dcMeta.getClusters().values().forEach(clusterMeta -> {
            if (interestedTypes.contains(clusterMeta.getType())) {
                interestedClusters.add(newClusterMeta(clusterMeta));
            }
        });

        DcMeta dcMetaWithInterestedClusters = new DcMeta(dcMeta.getId());
        interestedClusters.forEach(clusterMeta -> {
            dcMetaWithInterestedClusters.addCluster(clusterMeta);
        });
        return dcMetaWithInterestedClusters;
    }

    ClusterMeta outerClusterToInner(OuterClientService.ClusterMeta outer) {
        ClusterMeta clusterMeta = new ClusterMeta(outer.getName());
        if (outer.getClusterType().equals(OuterClientService.ClusterType.SINGEL_DC)) {
            clusterMeta.setType(ClusterType.SINGLE_DC.name());
            clusterMeta.setActiveDc(currentDcId);
        } else {
            clusterMeta.setType(ClusterType.LOCAL_DC.name());
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
    }

    ClusterMeta newClusterMeta(ClusterMeta origin) {
        ClusterMeta clusterMeta = new ClusterMeta(origin.getId()).setType(origin.getType()).setAdminEmails(origin.getAdminEmails()).setOrgId(origin.getOrgId());
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

    String innerClusterType(OuterClientService.ClusterType clusterType) {
        switch (clusterType) {
            case SINGEL_DC:
                return ClusterType.SINGLE_DC.name();
            case LOCAL_DC:
                return ClusterType.LOCAL_DC.name();
            case XPIPE_ONE_WAY:
                return ClusterType.ONE_WAY.name();
            case XPIPE_BI_DIRECT:
                return ClusterType.BI_DIRECTION.name();
        }
        return null;
    }

}
