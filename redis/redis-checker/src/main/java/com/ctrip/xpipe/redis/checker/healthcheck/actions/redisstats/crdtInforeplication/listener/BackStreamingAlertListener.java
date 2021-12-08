package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication.listener;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractMetricListener;

import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication.CrdtInfoReplicationContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication.CrdtInfoReplicationListener;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class BackStreamingAlertListener extends AbstractMetricListener<CrdtInfoReplicationContext, HealthCheckAction> implements CrdtInfoReplicationListener, BiDirectionSupport {
    private AlertManager alertManager;

    @Autowired
    public BackStreamingAlertListener(AlertManager alertManager) {
        this.alertManager = alertManager;
    }

    @Override
    public void onAction(CrdtInfoReplicationContext context) {
        InfoResultExtractor extractor = context.getResult();
        String backStreamingStats = extractor.extract("backstreaming");
        if (!StringUtil.isEmpty(backStreamingStats) && backStreamingStats.trim().equals("1")) {
            RedisInstanceInfo info = context.instance().getCheckInfo();
            logger.info("[BackStreamingAlertListener][{}][{}][{}] back streaming", info.getClusterId(), info.getShardId(), info.getHostPort());
            alertManager.alert(info, ALERT_TYPE.CRDT_BACKSTREAMING, info.getHostPort().toString());
        }
    }
}
