package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.redis.console.cluster.ConsoleCrossDcServer;
import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderElector;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.notifier.cluster.ClusterTypeUpdateEventFactory;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MetaSynchronizer {

    private ConsoleCrossDcServer consoleCrossDcServer;

    private FoundationService foundationService;

    Map<String, DcMetaSynchronizer> dcMetaSynchronizers;

    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("XPipe-Meta-Sync"));;

    private static Logger logger = LoggerFactory.getLogger(MetaSynchronizer.class);

    private OuterClientService outerClientService = OuterClientService.DEFAULT;

    @Autowired
    public MetaSynchronizer(ConsoleConfig consoleConfig, ConsoleLeaderElector leaderElector,
                            MetaCache metaCache, RedisService redisService, ShardService shardService,
                            ClusterService clusterService, DcService dcService, OrganizationService organizationService,
                            SentinelBalanceService sentinelBalanceService, ClusterTypeUpdateEventFactory clusterTypeUpdateEventFactory,
                            FoundationService foundationService, ConsoleCrossDcServer consoleCrossDcServer

    ) {
        this.consoleConfig = consoleConfig;
        this.consoleLeaderElector = leaderElector;
        this.metaCache = metaCache;
        this.redisService = redisService;
        this.shardService = shardService;
        this.clusterService = clusterService;
        this.dcService = dcService;
        this.organizationService = organizationService;
        this.sentinelBalanceService = sentinelBalanceService;
        this.clusterTypeUpdateEventFactory = clusterTypeUpdateEventFactory;
        this.foundationService = foundationService;
        this.consoleCrossDcServer = consoleCrossDcServer;
        dcMetaSynchronizers = new HashMap<>();
    }

    private MetaCache metaCache;

    private RedisService redisService;

    private ShardService shardService;

    private ClusterService clusterService;

    private DcService dcService;

    private OrganizationService organizationService;

    private ConsoleConfig consoleConfig;

    private ConsoleLeaderElector consoleLeaderElector;

    private SentinelBalanceService sentinelBalanceService;

    private ClusterTypeUpdateEventFactory clusterTypeUpdateEventFactory;

    @PostConstruct
    public void postConstruct() {
        scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                sync();
            }
        }, consoleConfig.getOuterClientSyncInterval(), consoleConfig.getOuterClientSyncInterval(), TimeUnit.MILLISECONDS);
    }

    public void sync() {
        Set<String> dcs = consoleConfig.getExtraSyncDC();
        for(String dc : dcs) {
            if(consoleCrossDcServer.amILeader()) {
                if(!dcMetaSynchronizers.containsKey(dc)) {
                    // meta.sync.external.dc 下dc 同步任务交给 cross dc leader
                    dcMetaSynchronizers.put(dc, new DcMetaSynchronizer(consoleConfig, metaCache, redisService, shardService,
                            clusterService, dcService, organizationService, sentinelBalanceService,
                            clusterTypeUpdateEventFactory, outerClientService, dc));
                    dcMetaSynchronizers.get(dc).start();

                }
            } else {
                if(dcMetaSynchronizers.containsKey(dc)) {
                    dcMetaSynchronizers.get(dc).stop();
                    dcMetaSynchronizers.remove(dc);
                }
            }

        }
        String currentDc = foundationService.getDataCenter();
        if(dcs.contains(currentDc)) {
            if(dcMetaSynchronizers.containsKey(currentDc)) {
                dcMetaSynchronizers.get(currentDc).stop();
                dcMetaSynchronizers.remove(currentDc);
            }
            return;
        }
        if(consoleLeaderElector.amILeader()) {
            if(!dcMetaSynchronizers.containsKey(currentDc)) {
                // 没有被cross dc leader 托管需要自己完成
                dcMetaSynchronizers.put(currentDc, new DcMetaSynchronizer(consoleConfig, metaCache, redisService, shardService,
                        clusterService, dcService, organizationService, sentinelBalanceService,
                        clusterTypeUpdateEventFactory, outerClientService,currentDc));
                dcMetaSynchronizers.get(currentDc).start();
            }
        } else {
            if(dcMetaSynchronizers.containsKey(currentDc)) {
                dcMetaSynchronizers.get(currentDc).stop();
                dcMetaSynchronizers.remove(currentDc);
            }
        }
        // 检查 被移除托管的 dc
        Set<String> needRemoved = new HashSet<>();
        for(String dc : dcMetaSynchronizers.keySet()) {
            if(!dcs.contains(dc) && !StringUtil.trimEquals(dc, currentDc)) {
                needRemoved.add(dc);
            }
        }

        for(String dc : needRemoved) {
            dcMetaSynchronizers.get(dc).stop();
            dcMetaSynchronizers.remove(dc);
        }
    }

}
