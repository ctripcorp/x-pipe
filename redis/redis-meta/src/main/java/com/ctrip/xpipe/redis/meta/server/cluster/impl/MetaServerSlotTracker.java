package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

@Component
public class MetaServerSlotTracker extends AbstractLifecycle implements Lifecycle, TopElement {

    public static final int ORDER = 0;

    private static final Logger logger = LoggerFactory.getLogger(MetaServerSlotTracker.class);

    private MetricProxy metricProxy = MetricProxy.DEFAULT;

    @Resource(name = SCHEDULED_EXECUTOR)
    private ScheduledExecutorService schedule;

    private SlotManager slotManager;

    private CurrentClusterServer currentClusterServer;

    private ScheduledFuture<?> future;


    @Autowired
    public MetaServerSlotTracker(SlotManager slotManager, CurrentClusterServer currentClusterServer) {
        this.slotManager = slotManager;
        this.currentClusterServer = currentClusterServer;
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();

    }

    @Override
    protected void doStop() throws Exception {

        if(future != null){
            future.cancel(true);
        }
        logger.info("[MetaSlot] stop");
        super.doStop();
    }

    @Override
    protected void doStart() throws Exception {
        logger.info("[MetaSlot] start");
        future = schedule.scheduleAtFixedRate(this::doTrackSlot,
                0, 1, TimeUnit.MINUTES);
    }

    @Override
    public int getOrder() {
        return ORDER;
    }

    private void doTrackSlot() {
        try {
            trackSlot();
        } catch (Throwable th) {
            logger.error("[doTrack]" + th.getMessage(), th);
        }
    }

    private void trackSlot() {
        Set<Integer> slots = currentClusterServer.slots();
        String serverId = String.valueOf(currentClusterServer.getServerId());
        long timeMillis =  System.currentTimeMillis();
        for(Integer slot : slots) {
            trackSlot(slot, serverId, timeMillis);
        }
    }

    private void trackSlot(int index, String serverId, long timeStamp) {

        MetricData metricData = new MetricData("meta_slots");
        metricData.setValue(1);
        metricData.setTimestampMilli(timeStamp);
        metricData.addTag("serverId", serverId);
        metricData.addTag("slot", String.valueOf(index));
        try {
            metricProxy.writeBinMultiDataPoint(metricData);
        } catch (Throwable th) {
            logger.debug("[tryMetric] fail", th);
        }
    }

}
