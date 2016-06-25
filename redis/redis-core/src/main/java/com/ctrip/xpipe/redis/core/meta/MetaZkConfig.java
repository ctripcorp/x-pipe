package com.ctrip.xpipe.redis.core.meta;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public class MetaZkConfig {
	
	public static String  getMetaRootPath(){
		return System.getProperty("zkMetaStoragePath", "/meta");
	}
	
}
