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
        return new EntrySetWrapper(inner.entrySet());
    }

    public static class EntrySetWrapper implements Set<Entry<Object, List<Object>>> {

        private final Set<Entry<CRedisSessionChannel, List<Object>>> inner;

        public EntrySetWrapper(Set<Entry<CRedisSessionChannel, List<Object>>> inner) {

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
        public boolean contains(Object o) {
            return inner.contains(o);
        }

        @Override
        public Iterator<Entry<Object, List<Object>>> iterator() {
            return new IteratorWrapper(inner.iterator());
        }

        @Override
        public Object[] toArray() {
            return inner.toArray();
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return inner.toArray(a);
        }

        @Override
        public boolean add(Entry<Object, List<Object>> objectListEntry) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return inner.contains(c);
        }

        @Override
        public boolean addAll(Collection<? extends Entry<Object, List<Object>>> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }
    }

    public static class IteratorWrapper implements Iterator<Entry<Object, List<Object>>> {

        private final Iterator<Entry<CRedisSessionChannel, List<Object>>> inner;

        public IteratorWrapper(Iterator<Entry<CRedisSessionChannel, List<Object>>> inner) {
            this.inner = inner;
        }

        @Override
        public boolean hasNext() {
            return inner.hasNext();
        }

        @Override
        public Entry<Object, List<Object>> next() {
            return new EntryWrapper(inner.next());
        }
    }

    public static class EntryWrapper implements Entry<Object, List<Object>> {

        private final Entry<CRedisSessionChannel, List<Object>> inner;

        public EntryWrapper(Entry<CRedisSessionChannel, List<Object>> inner) {
            this.inner = inner;
        }

        @Override
        public Object getKey() {
            return inner.getKey();
        }

        @Override
        public List<Object> getValue() {
            return inner.getValue();
        }

        @Override
        public List<Object> setValue(List<Object> value) {
            return inner.setValue(value);
        }
    }


}
