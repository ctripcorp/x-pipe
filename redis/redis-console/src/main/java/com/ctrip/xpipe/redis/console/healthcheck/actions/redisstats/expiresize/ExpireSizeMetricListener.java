package com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.expiresize;

import com.ctrip.xpipe.redis.console.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.AbstractLongValueMetricListener;
import org.springframework.stereotype.Component;

@Component
public class ExpireSizeMetricListener extends AbstractLongValueMetricListener<ExpireSizeActionContext> implements ExpireSizeCheckListener, BiDirectionSupport {

    protected static final String METRIC_TYPE = "redis.expiresize";

    @Override
    protected String getMetricType() {
        return METRIC_TYPE;
    }

}
