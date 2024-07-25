package com.ctrip.xpipe.track;

import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

public abstract class AbstractLeaderTracker extends AbstractStartStoppable {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected MetricProxy metricProxy = MetricProxy.DEFAULT;

    @Resource(name = SCHEDULED_EXECUTOR)
    private ScheduledExecutorService schedule;

    private ScheduledFuture<?> future;

    private String metricType;

    public AbstractLeaderTracker(String metricType) {
        this.metricType = metricType;
    }

    @Override
    public void doStart() {
        logger.info("[startTracker]");
        future = schedule.scheduleAtFixedRate(this::doTrack,
                0, 1, TimeUnit.MINUTES);
    }

    @Override
    public void doStop() {
        logger.info("[endTracker]");
        if(future != null) {
            future.cancel(true);
            future = null;
        }
    }

    public void doTrack() {
        try {
            track();
        } catch (Throwable e) {
            logger.error("[doTrack]", e);
        }
    }

    protected void track() {
        MetricData metricData = new MetricData(metricType);
        metricData.setValue(1);
        metricData.setTimestampMilli(System.currentTimeMillis());
        addTages(metricData);
        try {
            metricProxy.writeBinMultiDataPoint(metricData);
        } catch (Throwable th) {
            logger.debug("[tryMetric] fail", th);
        }
    }

    protected void addTages(MetricData metricData) {
        // default do nothing
    }
}
