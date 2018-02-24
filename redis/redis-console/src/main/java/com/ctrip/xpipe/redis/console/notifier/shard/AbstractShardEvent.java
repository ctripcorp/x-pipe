package com.ctrip.xpipe.redis.console.notifier.shard;

import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Feb 08, 2018
 */
public abstract class AbstractShardEvent implements ShardEvent {

    private String shardName;

    private String clusterName;

    private String shardMonitorName;

    private String shardSentinels;

    private List<Observer> observers;

    private ExecutorService executor;

    protected AbstractShardEvent() {
        this.observers = Lists.newArrayListWithExpectedSize(5);
    }

    protected AbstractShardEvent(String clusterName, String shardName, ExecutorService executor) {
        this.clusterName = clusterName;
        this.shardName = shardName;
        this.executor = executor;
        this.observers = Lists.newArrayListWithExpectedSize(5);
    }

    @Override
    public String getShardName() {
        return shardName;
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }

    @Override
    public String getShardSentinels() {
        return shardSentinels;
    }

    @Override
    public String getShardMonitorName() {
        return shardMonitorName;
    }

    @Override
    public void addObserver(Observer observer) {
        synchronized (this) {
            observers.add(observer);
        }
    }

    @Override
    public void removeObserver(Observer observer) {
        synchronized (this) {
            observers.remove(observer);
        }
    }

    public AbstractShardEvent setShardName(String shardName) {
        this.shardName = shardName;
        return this;
    }

    public AbstractShardEvent setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public AbstractShardEvent setShardMonitorName(String shardMonitorName) {
        this.shardMonitorName = shardMonitorName;
        return this;
    }

    public AbstractShardEvent setShardSentinels(String shardSentinels) {
        this.shardSentinels = shardSentinels;
        return this;
    }

    @Override
    public void onEvent() {
        for(Observer observer : observers) {
            executor.execute(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() throws Exception {
                    logger.info("[onEvent] execute observer: {}", observer.getClass());
                    observer.update(getShardEventType(), getSelf());
                }
            });
        }
    }

    @Override
    public String toString() {
        return clusterName + "-" + shardName + "-" + shardMonitorName + "-" + shardSentinels;
    }

    protected abstract ShardEvent getSelf();
}
