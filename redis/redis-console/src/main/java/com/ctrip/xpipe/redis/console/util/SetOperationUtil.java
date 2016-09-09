package com.ctrip.xpipe.redis.console.util;

import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

/**
 * @author shyin
 *
 * Sep 1, 2016
 */
public class SetOperationUtil {
	public <T> Collection<T> difference(Class<T> clazz,Collection<T> collection1, Collection<T> collection2, Comparator<T> comparator) {
		List<T> difference = new LinkedList<T>();
		
		if(null != collection1) {
			for(T itemInFirst : collection1) {
				if(null != collection2) {
					boolean exist = false;
					for(T itemInSecond : collection2) {
						if(0 == comparator.compare(itemInFirst, itemInSecond)) {
							exist = true;
							break;
						}
					}
					if(!exist) {
						difference.add(itemInFirst);
					}
				} else {
					difference.add(itemInFirst);
				}
			}
		}
		
		return difference;
	}
	
	public <T> Collection<T> intersection(Class<T> clazz, Collection<T> collection1, Collection<T> collection2, Comparator<T> comparator) {
		List<T> interesction = new LinkedList<T>();
		
		if(null != collection1) {
			for(T itemInFirst : collection1) {
				if(null != collection2) {
					for(T itemInSecond : collection2) {
						if(0 == comparator.compare(itemInFirst, itemInSecond)) {
							interesction.add(itemInFirst);
							break;
						}
					}
				}
			}
		}
		
		return interesction;
	}
}
