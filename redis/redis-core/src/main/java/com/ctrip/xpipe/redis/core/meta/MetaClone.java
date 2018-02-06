package com.ctrip.xpipe.redis.core.meta;

import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * @author wenchao.meng
 *
 * Aug 5, 2016
 */
public class MetaClone {

	private static Logger logger = LoggerFactory.getLogger(MetaClone.class);
	
	public static <T extends Serializable> T clone(T obj){
		try {
			return SerializationUtils.clone(obj);
		}catch (SerializationException e){
			logger.error("[clone]", e);
			throw e;
		}
	}

}
