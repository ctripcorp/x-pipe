package com.ctrip.xpipe.redis.console.notifier.cluster;

import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardEvent;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Feb 11, 2018
 */
public abstract class AbstractClusterEvent implements ClusterEvent {

    private String clusterName;

    private long orgId;

    private List<ShardEvent> shardEvents;

    private List<Observer> observers;

    private ExecutorService executor;

    private ClusterType clusterType;

    protected AbstractClusterEvent(String clusterName, long orgId, ExecutorService executor) {
        this.clusterName = clusterName;
        this.orgId = orgId;
        this.executor = executor;
        this.shardEvents = Lists.newLinkedList();
        this.observers = Lists.newArrayListWithExpectedSize(5);
    }

    @Override
    public String getClusterName() {
        return this.clusterName;
    }

    @Override
    public long getOrgId() {
        return this.orgId;
    }

    @Override
    public List<ShardEvent> getShardEvents() {
        return shardEvents;
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

    @Override
    public void onEvent() {
        for(Observer observer : observers) {
            executor.execute(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() throws Exception {
                    getLogger().info("[onEvent] execute observer: {}", observer.getClass());
                    observer.update(getClusterEventType(), getSelf());
                }
            });
        }
    }

    @Override
    public String toString() {
        return getSelf().getClass().getSimpleName() + ": " + clusterName;
    }

    @Override
    public void addShardEvent(ShardEvent shardEvent) {
        if(shardEvent == null || StringUtil.isEmpty(shardEvent.getClusterName())) {
            return;
        }
        if(shardEvent.getShardEventType() != getClusterEventType()) {
            throw new IllegalArgumentException(String.format("cluster event type is %s, but shard event type is %s",
                    getClusterEventType(), shardEvent.getShardEventType()));
        }
        if(!shardEvent.getClusterName().equals(this.clusterName)) {
            throw new IllegalArgumentException(String.format("cluster name should be %s, but shard's cluster name is %s",
                    clusterName, shardEvent.getClusterName()));
        }

        this.shardEvents.add(shardEvent);
    }

    public void addShardEvents(Collection<ShardEvent> shardEvents) {
        for(ShardEvent shardEvent : shardEvents) {
            addShardEvent(shardEvent);
        }
    }

    public void setClusterType(ClusterType clusterType) {
        this.clusterType = clusterType;
    }

    @Override
    public ClusterType getClusterType() {
        return clusterType;
    }

    protected abstract ClusterEvent getSelf();
}
