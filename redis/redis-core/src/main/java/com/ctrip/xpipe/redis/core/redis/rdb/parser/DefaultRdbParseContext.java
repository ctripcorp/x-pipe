package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseListener;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.google.common.collect.Maps;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author lishanglin
 * date 2022/5/29
 */
public class DefaultRdbParseContext implements RdbParseContext {

    private EnumMap<RdbType, RdbParser> parsers = new EnumMap<>(RdbType.class);

    private List<RdbParseContext> otherParsers = new LinkedList<>();

    private Set<RdbParseListener> listeners = new HashSet<>();

    private AtomicInteger dbId = new AtomicInteger();

    private AtomicInteger version = new AtomicInteger();

    private AtomicReference<RdbType> currentType = new AtomicReference<>();

    private Map<String, String> auxMap = Maps.newConcurrentMap();

    private AtomicReference<RedisKey> redisKey = new AtomicReference<>();

    private AtomicLong expireMilli = new AtomicLong();

    private AtomicLong lruIdle = new AtomicLong(-1);

    private AtomicInteger lfuFreq = new AtomicInteger(-1);

    @Override
    public synchronized void bindRdbParser(RdbParser<?> parser) {
        listeners.forEach(parser::registerListener);
    }

    @Override
    public RdbParser getOrCreateParser(RdbType rdbType) {
        RdbParser parser = parsers.get(rdbType);
        if (null != parser) return parser;

        synchronized (this) {
            if (parsers.containsKey(rdbType)) return parsers.get(rdbType);
            RdbParser newParser = rdbType.makeParser(this);
            listeners.forEach(newParser::registerListener);
            parsers.put(rdbType, newParser);
            return parsers.get(rdbType);
        }
    }

    @Override
    public void registerListener(RdbParseListener listener) {
        if (listeners.contains(listener)) return;

        synchronized (this) {
            if (listeners.add(listener)) {
                parsers.values().forEach(parser -> parser.registerListener(listener));
                otherParsers.forEach(parser -> parser.registerListener(listener));
            }
        }
    }

    @Override
    public void unregisterListener(RdbParseListener listener) {
        if (!listeners.contains(listener)) return;

        synchronized (this) {
            if (listeners.remove(listener)) {
                parsers.values().forEach(parser -> parser.unregisterListener(listener));
                otherParsers.forEach(parser -> parser.unregisterListener(listener));
            }
        }
    }

    @Override
    public RdbParseContext setDbId(int dbId) {
        this.dbId.set(dbId);
        return this;
    }

    @Override
    public int getDbId() {
        return this.dbId.get();
    }

    @Override
    public RdbParseContext setRdbVersion(int version) {
        this.version.set(version);
        return this;
    }

    @Override
    public int getRdbVersion() {
        return version.get();
    }

    @Override
    public RdbParseContext setCurrentType(RdbType rdbType) {
        this.currentType.set(rdbType);
        return this;
    }

    @Override
    public RdbType getCurrentType() {
        return currentType.get();
    }

    @Override
    public RdbParseContext setAux(String key, String value) {
        this.auxMap.put(key, value);
        return this;
    }

    @Override
    public String getAux(String key) {
        return auxMap.get(key);
    }

    @Override
    public RdbParseContext setKey(RedisKey key) {
        this.redisKey.set(key);
        return this;
    }

    @Override
    public RedisKey getKey() {
        return this.redisKey.get();
    }

    @Override
    public RdbParseContext setExpireMilli(long expireMilli) {
        this.expireMilli.set(expireMilli);
        return this;
    }

    @Override
    public long getExpireMilli() {
        return this.expireMilli.get();
    }

    @Override
    public RdbParseContext setLruIdle(long idle) {
        this.lruIdle.set(idle);
        return this;
    }

    @Override
    public long getLruIdle() {
        return this.lruIdle.get();
    }

    @Override
    public RdbParseContext setLfuFreq(int freq) {
        this.lfuFreq.set(freq);
        return this;
    }

    @Override
    public int getLfuFreq() {
        return this.lfuFreq.get();
    }

    @Override
    public void clearKvContext() {
        this.redisKey.set(null);
        this.currentType.set(null);
        this.expireMilli.set(0);
        this.lruIdle.set(-1);
        this.lfuFreq.set(-1);
    }

    @Override
    public void reset() {
        parsers.values().forEach(RdbParser::reset);
    }
}
