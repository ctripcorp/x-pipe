package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.tombstonesize;

import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractLongValueMetricListener;
import org.springframework.stereotype.Component;

@Component
public class TombstoneSizeMetricListener extends AbstractLongValueMetricListener<TombstoneSizeActionContext> implements TombstoneSizeCheckListener, BiDirectionSupport {

    protected static final String METRIC_TYPE = "redis.tombstonesize";

    @Override
    protected String getMetricType() {
        return METRIC_TYPE;
    }

}
