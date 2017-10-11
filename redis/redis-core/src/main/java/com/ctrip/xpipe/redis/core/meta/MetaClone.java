package com.ctrip.xpipe.redis.core.meta;

import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;

/**
 * @author wenchao.meng
 *
 * Aug 5, 2016
 */
public class MetaClone {
	
	public static <T extends Serializable> T clone(T obj){
		
		return SerializationUtils.clone(obj);
	} 

}
