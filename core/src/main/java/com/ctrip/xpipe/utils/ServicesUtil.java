package com.ctrip.xpipe.utils;

import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.foundation.FoundationService;

/**
 * @author wenchao.meng
 *
 * Jun 13, 2016
 */
public class ServicesUtil {
	
	private static Logger logger = LoggerFactory.getLogger(ServicesUtil.class);
	
	public static FoundationService getFoundationService(){
		
		return load(FoundationService.class);
	}
	
	private static <T> T load(Class<T> clazz) {
		
		ServiceLoader<T> services = ServiceLoader.load(clazz);
		
		T result = null;
		
		int i = 0;
		for(T service : services){
			
			result = service;
			i++;
			logger.info("[load]{}, {}", service.getClass(), service);
		}
		
		if(i == 0){
			throw new IllegalStateException("service not found:" + clazz.getClass().getSimpleName() + ", "
					+ "if you work in ctrip, add ctrip-service project in your classpath, otherwise implement your own service");
		}
		
		if(i > 1){
			throw new IllegalStateException("service found more than once");
		}
		
		return result;
	}
}
