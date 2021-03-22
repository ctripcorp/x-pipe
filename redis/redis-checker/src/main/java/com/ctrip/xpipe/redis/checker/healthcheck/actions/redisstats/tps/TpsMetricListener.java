package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.tps;

import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractLongValueMetricListener;
import org.springframework.stereotype.Component;

@Component
public class TpsMetricListener extends AbstractLongValueMetricListener<TpsActionContext> implements TpsCheckListener, BiDirectionSupport {

    protected static final String METRIC_TYPE = "redis.tps";

    @Override
    protected String getMetricType() {
        return METRIC_TYPE;
    }

}
