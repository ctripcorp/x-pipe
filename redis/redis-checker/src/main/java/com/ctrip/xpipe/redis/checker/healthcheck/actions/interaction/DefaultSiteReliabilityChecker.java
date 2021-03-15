//package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;
//
//import com.ctrip.xpipe.endpoint.HostPort;
//import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
//import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.AbstractInstanceEvent;
//import com.ctrip.xpipe.redis.core.meta.MetaCache;
//import com.ctrip.xpipe.utils.VisibleForTesting;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Component;
//
//import java.util.List;
//
///**
// * @author chen.zhu
// * <p>
// * Sep 18, 2018
// */
//@Component
//public class DefaultSiteReliabilityChecker implements SiteReliabilityChecker {
//
//    @Autowired
//    private MetaCache metaCache;
//
//    @Autowired
//    private CheckerConfig config;
//
//    @Autowired
//    private DefaultDelayPingActionCollector defaultDelayPingActionCollector;
//
//    @Override
//    public boolean isSiteHealthy(AbstractInstanceEvent event) {
//        if(config.isConsoleSiteUnstable()) {
//            return false;
//        }
//        RedisInstanceInfo info = event.getInstance().getCheckInfo();
//        List<HostPort> totalRedis = metaCache.getAllActiveRedisOfDc(FoundationService.DEFAULT.getDataCenter(), info.getDcId());
//        int errorRedis = getErrorRedis(totalRedis);
//        return errorRedis < totalRedis.size()/2;
//    }
//
//    private int getErrorRedis(List<HostPort> totalRedis) {
//        int count = 0;
//        for(HostPort redis : totalRedis) {
//            HEALTH_STATE state = defaultDelayPingActionCollector.getState(redis);
//            if(!state.equals(HEALTH_STATE.INSTANCEUP) && !state.equals(HEALTH_STATE.HEALTHY)) {
//                count ++;
//            }
//        }
//        return count;
//    }
//
//    @VisibleForTesting
//    public DefaultSiteReliabilityChecker setMetaCache(MetaCache metaCache) {
//        this.metaCache = metaCache;
//        return this;
//    }
//}
