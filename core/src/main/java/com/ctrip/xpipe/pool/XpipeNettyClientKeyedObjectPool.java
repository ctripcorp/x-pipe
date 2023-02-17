package com.ctrip.xpipe.pool;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.pool.ObjectPoolException;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.NettyClientFactoryFromKeyed;
import com.ctrip.xpipe.netty.commands.NettyKeyedPoolClientFactory;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.core.Ordered;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * @author chen.zhu
 * <p>
 * Sep 13, 2018
 */
public class XpipeNettyClientKeyedObjectPool extends AbstractLifecycle
        implements TopElement, SimpleKeyedObjectPool<Endpoint, NettyClient> {

    public static final long DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS = 30000;
    public static final long DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS = 1000L * 60L * 30L;
    public static final long DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS = 5000L;

    private ConcurrentMap<Endpoint, XpipeNettyClientPool> objectPools = Maps.newConcurrentMap();
    private NettyKeyedPoolClientFactory pooledObjectFactory;
    private GenericObjectPoolConfig config;

    public XpipeNettyClientKeyedObjectPool() {
        this(createDefaultConfig(GenericKeyedObjectPoolConfig.DEFAULT_MAX_TOTAL_PER_KEY));
    }

    public XpipeNettyClientKeyedObjectPool(int maxPerKey) {
        this(createDefaultConfig(maxPerKey));
    }

    private static GenericObjectPoolConfig createDefaultConfig(int maxPerKey) {
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setJmxEnabled(false);
        config.setMaxTotal(maxPerKey);
        config.setBlockWhenExhausted(false);
        return config;
    }

    public XpipeNettyClientKeyedObjectPool(GenericObjectPoolConfig config) {
        this.config = config;

    }

    public XpipeNettyClientKeyedObjectPool(NettyKeyedPoolClientFactory pooledObjectFactory) {
        this(createDefaultConfig(GenericKeyedObjectPoolConfig.DEFAULT_MAX_TOTAL_PER_KEY));
        this.pooledObjectFactory = pooledObjectFactory;
    }

    public XpipeNettyClientKeyedObjectPool(int maxPerKey, NettyKeyedPoolClientFactory pooledObjectFactory) {
        this(maxPerKey);
        this.pooledObjectFactory = pooledObjectFactory;
    }

    @Override
    protected void doInitialize() throws Exception {

        if(this.pooledObjectFactory == null) {
            this.pooledObjectFactory = new NettyKeyedPoolClientFactory();
        }
        pooledObjectFactory.start();

        config.setTestOnBorrow(true);
        config.setTestOnReturn(false);
        config.setJmxEnabled(false);
        config.setSoftMinEvictableIdleTimeMillis(DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
        config.setMinEvictableIdleTimeMillis(DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
        config.setTimeBetweenEvictionRunsMillis(DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS);
    }

    public final void setKeyPooConfig(int minIdlePerKey, long softMinEvictableIdleTimeMillis, long minEvictableIdleTimeMillis, long timeBetweenEvictionRunsMillis) {

        logger.info("[setKeyPooConfig]{}, {}, {}, {}", minIdlePerKey, softMinEvictableIdleTimeMillis, minEvictableIdleTimeMillis, timeBetweenEvictionRunsMillis);
        config.setMinIdle(minIdlePerKey);
        config.setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis);
        config.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
        config.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);

    }


    @Override
    public SimpleObjectPool<NettyClient> getKeyPool(Endpoint key){
        return getOrCreate(key);
    }

    @Override
    public NettyClient borrowObject(Endpoint key) throws BorrowObjectException {

        try {
            logger.debug("[borrowObject][begin]{}", key);
            NettyClient value = getOrCreate(key).borrowObject();
            logger.debug("[borrowObject][end]{}, {}", key, value);
            return value;
        } catch (Exception e) {
            logger.error("[borrowObject]" + key, e);
            throw new BorrowObjectException("borrow " + key, e);
        }
    }

    @Override
    public void returnObject(Endpoint key, NettyClient value) throws ReturnObjectException {

        try {
            logger.debug("[returnObject]{}, {}", key, value);
            getOrCreate(key).returnObject(value);
        } catch (Exception e) {
            logger.error("[returnObject]", e);
            throw new ReturnObjectException("return " + key + " " + value, e);
        }
    }

    @Override
    protected void doDispose() throws Exception {
        for(Map.Entry<Endpoint, XpipeNettyClientPool> entry : objectPools.entrySet()) {
            entry.getValue().dispose();
        }
        this.pooledObjectFactory.stop();

    }

    @Override
    public void clear() throws ObjectPoolException {
        try {
            for(Map.Entry<Endpoint, XpipeNettyClientPool> entry : objectPools.entrySet()) {
                entry.getValue().dispose();
            }
        } catch (Exception e) {
            throw new ObjectPoolException("clear ", e);
        }
    }

    @Override
    public void clear(Endpoint key) throws ObjectPoolException {
        XpipeNettyClientPool pool = objectPools.remove(key);
        if(pool == null) {
            logger.warn("[clear] clear an non-existing object pool");
            return;
        }
        try {
            pool.clear();
        } catch (Exception e) {
            throw new ObjectPoolException("object pool:" + getOrCreate(key) + ",key:" + key, e);
        }
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    private XpipeNettyClientPool getOrCreate(Endpoint key) {
        return MapUtils.getOrCreate(objectPools, key, new ObjectFactory<XpipeNettyClientPool>() {
            @Override
            public XpipeNettyClientPool create() {
                XpipeNettyClientPool clientPool =  new XpipeNettyClientPool(key, config,
                        new NettyClientFactoryFromKeyed(key, pooledObjectFactory));
                try {
                    LifecycleHelper.initializeIfPossible(clientPool);
                } catch (Exception e) {
                    logger.error("[create] endpoint: {}", key, e);
                }
                return clientPool;
            }
        });
    }

    @VisibleForTesting
    public ObjectPool getObjectPool(Endpoint endpoint) {
        return getOrCreate(endpoint).getObjectPool();
    }

    @VisibleForTesting
    protected XpipeNettyClientPool getClientPool(Endpoint endpoint) {
        return objectPools.get(endpoint);
    }

}
