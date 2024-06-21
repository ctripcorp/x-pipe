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
    private EnumMap<RdbCrdtType, RdbParser> crdtParsers = new EnumMap<>(RdbCrdtType.class);

    private List<String> incompatibleKeys = new LinkedList<>();

    private List<RdbParseContext> otherParsers = new LinkedList<>();

    private Set<RdbParseListener> listeners = new HashSet<>();

    private AtomicInteger dbId = new AtomicInteger(-1);

    private AtomicInteger version = new AtomicInteger(-1);

    private AtomicReference<RdbType> currentType = new AtomicReference<>();

    private Map<String, String> auxMap = Maps.newConcurrentMap();

    private AtomicReference<RedisKey> redisKey = new AtomicReference<>();
    private AtomicReference<RdbCrdtType> crdtType = new AtomicReference<>();
    private AtomicReference<Boolean> crdt = new AtomicReference<>(false);

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
    public RdbParser getOrCreateCrdtParser(RdbCrdtType rdbCrdtType) {
        RdbParser parser = crdtParsers.get(rdbCrdtType);
        if (null != parser) return parser;

        synchronized (this) {
            if (crdtParsers.containsKey(rdbCrdtType)) return crdtParsers.get(rdbCrdtType);
            RdbParser newParser = rdbCrdtType.makeParser(this);
            listeners.forEach(newParser::registerListener);
            crdtParsers.put(rdbCrdtType, newParser);
            return crdtParsers.get(rdbCrdtType);
        }
    }

    @Override
    public void registerListener(RdbParseListener listener) {
        if (listeners.contains(listener)) return;

        synchronized (this) {
            if (listeners.add(listener)) {
                parsers.values().forEach(parser -> parser.registerListener(listener));
                crdtParsers.values().forEach(parser -> parser.registerListener(listener));
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
                crdtParsers.values().forEach(parser -> parser.unregisterListener(listener));
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

    public Map<String, String> getAllAux() {
        return Collections.unmodifiableMap(auxMap);
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

    @Override
    public void setCrdt(boolean crdt) {
        this.crdt.set(crdt);
    }

    @Override
    public boolean isCrdt() {
        return crdt.get();
    }

    @Override
    public RdbParseContext setCrdtType(RdbCrdtType crdtType) {
        this.crdtType.set(crdtType);
        return this;
    }

    @Override
    public void setIncompatibleKey(String key) {
        incompatibleKeys.add(key);
    }

    @Override
    public List<String> getIncompatibleKey() {
        return incompatibleKeys;
    }

    @Override
    public RdbCrdtType getCrdtType() {
        return crdtType.get();
    }

    @Override
    public void clearCrdtType() {
        this.crdtType.set(null);
    }

    @Override
    public void clearIncompatibleKey() {
        incompatibleKeys.clear();
    }
}
