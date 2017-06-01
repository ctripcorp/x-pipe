package com.ctrip.xpipe.concurrent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author wenchao.meng
 *         <p>
 *         May 18, 2017
 */
public class FinalStateSetterManager<K, S> {

    private Map<K, S> map = new ConcurrentHashMap<K, S>();
    private Supplier<S> getter;
    private Consumer<S> setter;
    private Executors executors;

    public FinalStateSetterManager(Executors executors, Supplier<S> getter, Consumer<S> setter){
        this.getter = getter;
        this.setter = setter;
        this.executors = executors;
    }


    public void set(K k, S s){

        synchronized (map){
            map.put(k, s);
        }

    }


}
