package com.ctrip.xpipe.redis.console.health.action;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.metric.HostPort;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.health.HealthChecker;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.unidal.tuple.Pair;

import javax.annotation.PostConstruct;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *         <p>
 *         May 08, 2017
 */
@Component
@ConditionalOnProperty(name = { HealthChecker.ENABLED }, matchIfMissing = true)
public class OuterClientInstanceStateChecker {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1,
            XpipeThreadFactory.create(getClass().getSimpleName()));

    @Autowired
    private ConsoleConfig consoleConfig;

    private OuterClientService outerClientService = OuterClientService.DEFAULT;

    private Set<HostPort> alertWhileList = new HashSet<>();

    @Autowired
    private MetaCache metaCache;


    @PostConstruct
    public void posConstruct(){

        scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {

                doRefershWhiteList();

                doCheck();
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    private void doRefershWhiteList() {

        String alertWhileList = consoleConfig.getAlertWhileList();
        if(StringUtil.isEmpty(alertWhileList)){
            return;
        }

        Set<HostPort> tmpWhiteList = new HashSet<>();

        String[] split = alertWhileList.split("\\s*,\\s*");
        for(String sp : split){

            try{
                Pair<String, Integer> pair = IpUtils.parseSingleAsPair(sp);
                tmpWhiteList.add(new HostPort(pair.getKey(), pair.getValue()));
            }catch(Exception e){
                logger.error("[doRefershWhiteList]" + sp, e);
            }
        }
        if(this.alertWhileList == null || !this.alertWhileList.equals(tmpWhiteList)){
            logger.info("[doRefershWhiteList]{}", tmpWhiteList);
            this.alertWhileList = tmpWhiteList;

        }
    }

    protected void doCheck() {

        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if(xpipeMeta == null){
            logger.info("[doCheck][null]");
            return;
        }

        for(DcMeta dcMeta : xpipeMeta.getDcs().values()){
            for(ClusterMeta clusterMeta : dcMeta.getClusters().values()){
                for(ShardMeta shardMeta : clusterMeta.getShards().values()){
                    for(RedisMeta redisMeta : shardMeta.getRedises()){

                        HostPort hostPort = new HostPort(redisMeta.getIp(), redisMeta.getPort());
                        try {
                            boolean isUp = outerClientService.isInstanceUp(hostPort);
                            if(!isUp){
                                alert(dcMeta.getId(), clusterMeta.getId(), shardMeta.getId(), hostPort);
                            }
                        } catch (Exception e) {
                            logger.error("[doCheck]" + hostPort, e);
                        }
                    }
                }
            }
        }
    }

    private void alert(String dc, String cluster, String shard, HostPort hostPort) {

        logger.info("[alert][down]{}-{}-{}-{}", dc, cluster,shard, hostPort);

        if(alertWhileList.contains(hostPort)){
            logger.info("[alert][in white list]{}", hostPort);
            return;
        }
        EventMonitor.DEFAULT.logAlertEvent(String.format("credis_down:%s-%s-%s-%s", dc, cluster, shard, hostPort));
    }
}
