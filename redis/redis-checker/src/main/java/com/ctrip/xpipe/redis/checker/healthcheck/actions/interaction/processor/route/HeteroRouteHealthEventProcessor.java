package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.processor.route;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.HeteroInstanceLongDelay;
import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.tuple.Pair;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class HeteroRouteHealthEventProcessor extends AbstractRouteHealthEventProcessor implements OneWaySupport {

    @Override
    protected boolean supportEvent(AbstractInstanceEvent event) {
        return event instanceof HeteroInstanceLongDelay;
    }

    @Override
    protected ProxyTunnelInfo findProxyTunnelInfo(AbstractInstanceEvent event) {
        RedisInstanceInfo instanceInfo = event.getInstance().getCheckInfo();
        return proxyManager.getProxyTunnelInfo(instanceInfo.getDcId(),
                instanceInfo.getClusterId(), getSrcShardId(event), "UNSET");
    }

    @Override
    protected long isProbablyHealthyInXSeconds(AbstractInstanceEvent event) {
        return 0;
    }

    @Override
    protected void logEvent(AbstractInstanceEvent event) {
        EventMonitor.DEFAULT.logEvent("XPIPE.PROXY.CHAIN", String.format("[CLOSE-HETERO]%s: %s",
                event.getInstance().getCheckInfo().getDcId(), getSrcShardId(event)));
    }

    @Override
    protected Pair<String, String> identifierOfEvent(AbstractInstanceEvent event) {
        RedisInstanceInfo info = event.getInstance().getCheckInfo();
        return Pair.from(info.getDcId(), getSrcShardId(event));
    }

    private String getSrcShardId(AbstractInstanceEvent event){
        RedisInstanceInfo instanceInfo = event.getInstance().getCheckInfo();
        long shardDBId = ((HeteroInstanceLongDelay) event).getSrcShardDBId();
        return instanceInfo.getActiveDcAllShardIds().get(shardDBId);
    }


}
