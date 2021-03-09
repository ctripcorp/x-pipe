package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.backstreaming;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractMetricListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author lishanglin
 * date 2021/1/26
 */
@Component
public class BackStreamingAlertListener extends AbstractMetricListener<BackStreamingContext, HealthCheckAction> implements BackStreamingListener, BiDirectionSupport {

    private AlertManager alertManager;

    @Autowired
    public BackStreamingAlertListener(AlertManager alertManager) {
        this.alertManager = alertManager;
    }

    @Override
    public void onAction(BackStreamingContext context) {
        if (null != context.getResult() && context.getResult()) {
            RedisInstanceInfo info = context.instance().getCheckInfo();
            logger.info("[BackStreamingAlertListener][{}][{}][{}] back streaming", info.getClusterId(), info.getShardId(), info.getHostPort());
            alertManager.alert(info, ALERT_TYPE.CRDT_BACKSTREAMING, info.getHostPort().toString());
        }
    }

}
