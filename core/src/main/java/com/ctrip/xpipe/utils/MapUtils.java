package com.ctrip.xpipe.utils;

import com.ctrip.xpipe.api.factory.ObjectFactory;

import java.util.Map;

/**
 * @author wenchao.meng
 *
 * Jun 17, 2016
 */
public class MapUtils {
	
	
	public static  <K, V>  V getOrCreate(Map<K, V> map, K key, ObjectFactory<V>  objectFactory){
		
		V value = map.get(key);
		if(value != null){
			return value;
		}
		
		synchronized (map) {
			value = map.get(key);
			if(value == null){
				value = objectFactory.create();
				map.put(key, value);
			}
		}
		return value;
	}
	
	

}
