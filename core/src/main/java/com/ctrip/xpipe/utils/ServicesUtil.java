package com.ctrip.xpipe.utils;

import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.email.EmailService;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.api.migration.DcMapper;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.api.migration.auto.MonitorServiceFactory;
import com.ctrip.xpipe.api.organization.Organization;
import com.ctrip.xpipe.api.sso.LogoutHandler;
import com.ctrip.xpipe.api.sso.UserInfo;
import com.ctrip.xpipe.api.sso.UserInfoHolder;
import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.client.redis.AsyncRedisClientFactory;
import com.ctrip.xpipe.lifecycle.OrderedComparator;
import com.ctrip.xpipe.metric.MetricProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author wenchao.meng
 *
 * Jun 13, 2016
 */
public class ServicesUtil {
	
	private static Logger logger = LoggerFactory.getLogger(ServicesUtil.class);
	
	private static Map<Class<?>, Object> allServices = new ConcurrentHashMap<>();
	
	public static Config getConfigService(){
		return load(Config.class);
	}
	
	
	public static FoundationService getFoundationService(){
		
		return load(FoundationService.class);
	}

	public static LogoutHandler getLogoutHandler(){
		return load(LogoutHandler.class);
	}

	public static UserInfoHolder getUserInfoHolder(){
		return load(UserInfoHolder.class);
	}

	public static UserInfo getUserInfo(){
		return load(UserInfo.class);
	}
	
	public static MetricProxy getMetricProxy(){
		return load(MetricProxy.class);
	}
	
	public static OuterClientService getOuterClientService() {
		return load(OuterClientService.class);
	}

	public static DcMapper getDcMapperService() {
		return load(DcMapper.class);
	}

	public static Organization getOrganizationService() {return load(Organization.class);}

	public static EmailService getEmailService() {
		return load(EmailService.class);
	}

	public static AsyncRedisClientFactory getAsyncRedisClientFactory() {
	    return load(AsyncRedisClientFactory.class);
	}

	public static MonitorServiceFactory getMonitorServiceFactory() {
		return load(MonitorServiceFactory.class);
	}

	@SuppressWarnings("unchecked")
	public static <T extends Ordered> T load(Class<T> clazz) {
		
		T result = (T) allServices.get(clazz);
		
		if(result == null){
			synchronized (clazz) {
				
				result = (T) allServices.get(clazz);
				
				if(result == null){
					
					ServiceLoader<T> services = ServiceLoader.load(clazz);
					List<T> sortServices = new LinkedList<>();
					for(T service : services){
						logger.info("[load]{}, {}", service.getClass(), service);
						sortServices.add(service);
					}
					
					Collections.sort(sortServices, new OrderedComparator());
					
					if(sortServices.size() == 0){
						throw new IllegalStateException("service not found:" + clazz.getClass().getSimpleName() + ", "
								+ "if you work in ctrip, add ctrip-service project in your classpath, otherwise implement your own service");
					}
					result = sortServices.get(0);
					logger.info("[load][use]{}", result);
					allServices.put(clazz, result);
				}
			}
		}
		return result;
	}
}
