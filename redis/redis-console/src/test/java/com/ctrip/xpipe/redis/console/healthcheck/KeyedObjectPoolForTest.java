package com.ctrip.xpipe.redis.console.healthcheck;

import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.pool.ObjectPoolException;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.pool.BorrowObjectException;
import com.ctrip.xpipe.pool.ReturnObjectException;
import com.ctrip.xpipe.pool.XpipeObjectPoolFromKeyed;
import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.springframework.core.Ordered;

/**
 * @author chen.zhu
 * <p>
 * Sep 10, 2018
 */
public class KeyedObjectPoolForTest extends AbstractLifecycle
        implements TopElement, SimpleKeyedObjectPool<String, Object> {

    public static final long DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS = 30000;
    public static final long DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS = 1000L * 60L * 30L;
    public static final long DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS = 5000L;


    private KeyedObjectPool<String, Object> objectPool;
    private ObjectFactory pooledObjectFactory;
    private GenericKeyedObjectPoolConfig config;

    public KeyedObjectPoolForTest() {
        this(createDefaultConfig(GenericKeyedObjectPoolConfig.DEFAULT_MAX_TOTAL_PER_KEY));
    }

    public KeyedObjectPoolForTest(int maxPerKey) {
        this(createDefaultConfig(maxPerKey));
    }

    private static GenericKeyedObjectPoolConfig createDefaultConfig(int maxPerKey) {
        GenericKeyedObjectPoolConfig config = new GenericKeyedObjectPoolConfig();
        config.setMaxTotalPerKey(maxPerKey);
        config.setBlockWhenExhausted(false);
        return config;
    }

    public KeyedObjectPoolForTest(GenericKeyedObjectPoolConfig config) {
        this.config = config;

    }

    public KeyedObjectPoolForTest(ObjectFactory pooledObjectFactory) {
        this(createDefaultConfig(GenericKeyedObjectPoolConfig.DEFAULT_MAX_TOTAL_PER_KEY));
        this.pooledObjectFactory = pooledObjectFactory;
    }

    @Override
    protected void doInitialize() throws Exception {

        if (this.pooledObjectFactory == null) {
            this.pooledObjectFactory = new ObjectFactory();
        }

        GenericKeyedObjectPool<String, Object> genericKeyedObjectPool = new GenericKeyedObjectPool<String, Object>(
                pooledObjectFactory, config);
        genericKeyedObjectPool.setTestOnBorrow(true);
        genericKeyedObjectPool.setTestOnCreate(true);
        genericKeyedObjectPool.setSoftMinEvictableIdleTimeMillis(DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
        genericKeyedObjectPool.setMinEvictableIdleTimeMillis(DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
        genericKeyedObjectPool.setTimeBetweenEvictionRunsMillis(DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS);
        this.objectPool = genericKeyedObjectPool;
    }

    public final void setKeyPooConfig(int minIdlePerKey, long softMinEvictableIdleTimeMillis, long minEvictableIdleTimeMillis, long timeBetweenEvictionRunsMillis) {

        if (objectPool instanceof GenericKeyedObjectPool) {
            logger.info("[setKeyPooConfig]{}, {}, {}, {}", minIdlePerKey, softMinEvictableIdleTimeMillis, minEvictableIdleTimeMillis, timeBetweenEvictionRunsMillis);
            GenericKeyedObjectPool genericKeyedObjectPool = (GenericKeyedObjectPool) objectPool;
            genericKeyedObjectPool.setMinIdlePerKey(minIdlePerKey);
            genericKeyedObjectPool.setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis);
            genericKeyedObjectPool.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
            genericKeyedObjectPool.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
        } else {
            logger.warn("[setKeyPooConfig][not generickeyedobjectpool]");
        }
    }


    @Override
    public SimpleObjectPool<Object> getKeyPool(String key) {
        return new XpipeObjectPoolFromKeyed<String, Object>(this, key);
    }

    @Override
    public Object borrowObject(String key) throws BorrowObjectException {

        try {
            logger.debug("[borrowObject][begin]{}", key);
            Object value = this.objectPool.borrowObject(key);
            logger.debug("[borrowObject][end]{}, {}", key, value);
            return value;
        } catch (Exception e) {
            logger.error("[borrowObject]" + key, e);
            throw new BorrowObjectException("borrow " + key, e);
        }
    }

    @Override
    public void returnObject(String key, Object value) throws ReturnObjectException {

        try {
            logger.debug("[returnObject]{}, {}", key, value);
            this.objectPool.returnObject(key, value);
        } catch (Exception e) {
            logger.error("[returnObject]", e);
            throw new ReturnObjectException("return " + key + " " + value, e);
        }
    }

    @Override
    protected void doDispose() throws Exception {

        this.objectPool.close();

    }

    @Override
    public void clear() throws ObjectPoolException {
        try {
            this.objectPool.clear();
        } catch (Exception e) {
            throw new ObjectPoolException("clear " + objectPool, e);
        }
    }

    @Override
    public void clear(String key) throws ObjectPoolException {
        try {
            this.objectPool.clear(key);
        } catch (Exception e) {
            throw new ObjectPoolException("object pool:" + objectPool + ",key:" + key, e);
        }
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }


}

class ObjectFactory implements KeyedPooledObjectFactory<String, Object> {

    @Override
    public PooledObject<Object> makeObject(String key) throws Exception {
        return new DefaultPooledObject<Object> (new Object());
    }

    @Override
    public void destroyObject(String key, PooledObject<Object> p) throws Exception {

    }

    @Override
    public boolean validateObject(String key, PooledObject<Object> p) {
        return true;
    }

    @Override
    public void activateObject(String key, PooledObject<Object> p) throws Exception {

    }

    @Override
    public void passivateObject(String key, PooledObject<Object> p) throws Exception {

    }
}
