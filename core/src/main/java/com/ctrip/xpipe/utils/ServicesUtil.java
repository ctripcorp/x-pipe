package com.ctrip.xpipe.utils;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

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
	
	private static Map<Class<?>, Object> allServices = new ConcurrentHashMap<>();
	
	public static FoundationService getFoundationService(){
		
		return load(FoundationService.class);
	}
	
	@SuppressWarnings("unchecked")
	private static <T> T load(Class<T> clazz) {
		
		T result = (T) allServices.get(clazz);
		
		if(result == null){
			synchronized (clazz) {
				
				result = (T) allServices.get(clazz);
				
				if(result == null){
					ServiceLoader<T> services = ServiceLoader.load(clazz);
					
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
					allServices.put(clazz, result);
				}
			}
		}
		return result;
	}
}
