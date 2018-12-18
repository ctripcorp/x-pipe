package com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.impl;

import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.LeveledEmbededSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DefaultLeveledEmbededSet<T> implements LeveledEmbededSet<T> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultLeveledEmbededSet.class);

    private DefaultLeveledEmbededSet<T> superSet;

    private DefaultLeveledEmbededSet<T> subSet;

    private Set<T> localHoldings = Sets.newConcurrentHashSet();

    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    @Override
    public LeveledEmbededSet<T> getSuperSet() {
        return superSet;
    }

    @Override
    public LeveledEmbededSet<T> getSubSet() {
        return subSet;
    }

    @Override
    public LeveledEmbededSet<T> getThrough(int level) {
        DefaultLeveledEmbededSet<T> iter = (DefaultLeveledEmbededSet<T>)getRoot();
        while(level > 0) {
            DefaultLeveledEmbededSet<T> successor = (DefaultLeveledEmbededSet<T>) iter.getSubSet();
            if(successor == null) {
                successor = new DefaultLeveledEmbededSet<>();
                successor.setSuperSet(iter);
                iter.setSubSet(successor);
            }
            iter = successor;
            level --;
        }
        return iter;
    }

    @Override
    public Set<T> getCurrentSet() {
        Set<T> result = Sets.newHashSet();
        Lock lock = readWriteLock.readLock();
        try {
            lock.lock();
            if(getSubSet() != null) {
                result.addAll(getSubSet().getCurrentSet());
            }
            result.addAll(localHoldings);
        } catch (Exception e) {
            logger.error("[getCurrentSet]", e);
        } finally {
            lock.unlock();
        }
        return result;
    }

    @Override
    public void add(T t) {
        Lock lock = readWriteLock.writeLock();
        try {
            lock.lock();
            this.localHoldings.add(t);
        } catch (Exception e) {
            logger.error("[add]", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void remove(T t) {
        Lock lock = readWriteLock.writeLock();
        try {
            lock.lock();
            if(!this.localHoldings.remove(t)
                    && getSubSet() != null) {
                getSubSet().remove(t);
            }
        } catch (Exception e) {
            logger.error("[remove]", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void resume(T t, int level) {
        if(level < 0) {
            logger.warn("[resume] unsupported level: {}", level);
            return;
        }
        LeveledEmbededSet<T> root = getRoot();
        root.remove(t);
        getThrough(level).add(t);
    }

    private LeveledEmbededSet<T> getRoot() {
        LeveledEmbededSet<T> predecessor = getSuperSet(), iter = this;
        while(predecessor != null) {
            iter = predecessor;
            predecessor = iter.getSuperSet();
        }
        return iter;
    }

    public DefaultLeveledEmbededSet<T> setSuperSet(DefaultLeveledEmbededSet<T> superSet) {
        this.superSet = superSet;
        return this;
    }

    public DefaultLeveledEmbededSet<T> setSubSet(DefaultLeveledEmbededSet<T> subSet) {
        this.subSet = subSet;
        return this;
    }
}
