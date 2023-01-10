package com.ctrip.xpipe.redis.checker.healthcheck.actions.gtidgap;

import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractLongValueMetricListener;
import org.springframework.stereotype.Component;

@Component
public class MetricGapListener extends AbstractLongValueMetricListener<GtidGapCheckActionContext> implements GtidGapCheckActionListener, OneWaySupport {

    private static final String TYPE = "gtid.gap";

    @Override
    protected String getMetricType() {
        return TYPE;
    }

}
