package com.ctrip.xpipe.tuple;

import com.ctrip.xpipe.utils.ObjectUtils;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.Map;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 06, 2017
 */
@JsonDeserialize(using = PairDeserial.class)
public class Pair<K, V> implements Map.Entry<K, V>{

    private K key;

    private V value;

    public Pair(K key, V value){
        this.key = key;
        this.value = value;
    }

    public Pair(Map.Entry<K, V> entry){
        this.key = entry.getKey();
        this.value = entry.getValue();
    }

    public Pair(){}

    public static <K1, V1>  Pair<K1, V1> of(K1 key, V1 value){
        return new Pair<>(key, value);
    }

    @Override
    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public V setValue(V value) {
        V previous = this.value;
        this.value = value;
        return previous;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(key, value);
    }

    @Override
    public boolean equals(Object obj) {

        if(!(obj instanceof Pair)){
            return false;
        }

        Pair  other = (Pair) obj;
        if(!(ObjectUtils.equals(key, other.key))){
            return false;
        }

        if(!(ObjectUtils.equals(value, other.value))){
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return String.format("%s, %s", key, value);
    }

    public static <K, V> Pair<K, V> from(K key, V value) {

        return new Pair<>(key, value);
    }
}

