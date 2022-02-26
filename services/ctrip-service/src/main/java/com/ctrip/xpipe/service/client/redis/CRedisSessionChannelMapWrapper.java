package com.ctrip.xpipe.service.client.redis;

import credis.java.client.async.qclient.network.CRedisSessionChannel;

import java.util.*;

/**
 * @author Slight
 * <p>
 * Feb 26, 2022 5:14 PM
 */
public class CRedisSessionChannelMapWrapper implements Map<Object, List<Object>> {

    public final Map<CRedisSessionChannel, List<Object>> inner;

    public CRedisSessionChannelMapWrapper(Map<CRedisSessionChannel, List<Object>> inner) {

        this.inner = inner;
    }

    @Override
    public int size() {
        return inner.size();
    }

    @Override
    public boolean isEmpty() {
        return inner.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return inner.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return inner.containsValue(value);
    }

    @Override
    public List<Object> get(Object key) {
        return inner.get(key);
    }

    @Override
    public List<Object> put(Object key, List<Object> value) {
        return inner.put((CRedisSessionChannel) key, value);
    }

    @Override
    public List<Object> remove(Object key) {
        return inner.remove(key);
    }

    @Override
    public void putAll(Map<?, ? extends List<Object>> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        inner.clear();
    }

    @Override
    public Set<Object> keySet() {
        return new HashSet<>(inner.keySet());
    }

    @Override
    public Collection<List<Object>> values() {
        return inner.values();
    }

    @Override
    public Set<Entry<Object, List<Object>>> entrySet() {
        throw new UnsupportedOperationException();
    }
}
