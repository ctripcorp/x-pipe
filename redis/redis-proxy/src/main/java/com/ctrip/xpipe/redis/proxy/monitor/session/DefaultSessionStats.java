package com.ctrip.xpipe.redis.proxy.monitor.session;

import com.ctrip.xpipe.redis.core.monitor.BaseInstantaneousMetric;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.monitor.stats.AbstractStats;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author chen.zhu
 * <p>
 * Oct 29, 2018
 */
public class DefaultSessionStats extends AbstractStats implements SessionStats {

    private AtomicLong inputBytes = new AtomicLong(0L);

    private AtomicLong outputBytes = new AtomicLong(0L);

    private BaseInstantaneousMetric inputMetric = new BaseInstantaneousMetric();

    private BaseInstantaneousMetric outputMetric = new BaseInstantaneousMetric();

    private volatile long lastUpdateTime = System.currentTimeMillis();

    private AtomicBoolean flag = new AtomicBoolean(false);

    private List<AutoReadEvent> autoReadEvents = Lists.newLinkedList();

    public DefaultSessionStats(Session session, ScheduledExecutorService scheduled) {
        super(session, scheduled);
    }

    @Override
    public void increaseInputBytes(long bytes) {
        updateLastTime();
        inputBytes.getAndAdd(bytes);
    }

    @Override
    public void increaseOutputBytes(long bytes) {
        updateLastTime();
        outputBytes.getAndAdd(bytes);
    }

    @Override
    public long lastUpdateTime() {
        return lastUpdateTime;
    }

    private void updateLastTime() {
        lastUpdateTime = System.currentTimeMillis();
    }

    @Override
    public long getInputBytes() {
        return inputBytes.get();
    }

    @Override
    public long getOutputBytes() {
        return outputBytes.get();
    }

    @Override
    public long getInputInstantaneousBPS() {
        return inputMetric.getInstantaneousMetric();
    }

    @Override
    public long getOutputInstantaneousBPS() {
        return outputMetric.getInstantaneousMetric();
    }

    @Override
    public List<AutoReadEvent> getAutoReadEvents() {
        return autoReadEvents;
    }

    @Override
    protected void doStart() throws Exception {
        updateLastTime();
        super.doStart();
    }

    @Override
    protected void doTask() {
        inputMetric.trackInstantaneousMetric(inputBytes.get());
        outputMetric.trackInstantaneousMetric(outputBytes.get());
    }

    @Override
    protected int getCheckIntervalMilli() {
        return 100;
    }

    @Override
    public void onInit() {

    }

    @Override
    public void onEstablished() {

    }

    @Override
    public void onWritable() {
        if(flag.compareAndSet(true, false)) {
            autoReadEvents.get(autoReadEvents.size() - 1).setEndTime(System.currentTimeMillis());
        }
    }

    @Override
    public void onNotWritable() {
        if(flag.compareAndSet(false, true)) {
            autoReadEvents.add(new AutoReadEvent().setStartTime(System.currentTimeMillis()));
        }
    }

    @Override
    public String toString() {
        return "DefaultSessionStats{" +
                "inputBytes=" + inputBytes.get() +
                ", outputBytes=" + outputBytes.get() +
                ", inputMetric=" + inputMetric.getInstantaneousMetric() +
                ", outputMetric=" + outputMetric.getInstantaneousMetric() +
                ", lastUpdateTime=" + DateTimeUtils.timeAsString(lastUpdateTime) +
                ", autoReadEvents=" + Arrays.deepToString(autoReadEvents.toArray(new AutoReadEvent[0])) +
                '}';
    }
}
