package com.ctrip.xpipe.redis.meta.server.impl;

import java.lang.reflect.Method;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServers;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.SLOT_STATE;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfo;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardType;
import com.ctrip.xpipe.utils.ObjectUtils;

/**
 * @author wenchao.meng
 *
 *         Aug 4, 2016
 */
@Component
@Aspect
public class DispatchMoving {
	
	private Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	private SlotManager slotManager;

	@Autowired
	private CurrentClusterServer currentClusterServer;

	@Autowired
	private ClusterServers<MetaServer>  clusterServers;

	@Pointcut("@annotation(com.ctrip.xpipe.redis.core.cluster.ClusterMovingMethod)")
	public void pointcutMovingMethod() {
	}

	@Around("pointcutMovingMethod() && args(clusterId,..,forwardInfo)")
	public Object tryMovingDispatch(ProceedingJoinPoint joinpoint, String clusterId, ForwardInfo forwardInfo) throws Throwable {
		
		String targetMethodName = joinpoint.getSignature().getName();
		logger.info("[tryMovingDispatch]{}, {}", targetMethodName, clusterId);

		if(forwardInfo != null && forwardInfo.getType() == ForwardType.MOVING){
			logger.info("[tryMovingDispatch][isMoving][self process]{}, {}", targetMethodName, clusterId);
			return joinpoint.proceed();
		}
		
		MetaServer exportServer = exportServer(clusterId);
		if(exportServer == currentClusterServer){
			throw new IllegalStateException("export server should not be current " + exportServer);
		}
		
		if(exportServer != null){
			Object []args = joinpoint.getArgs();
			if(forwardInfo == null){
				forwardInfo = new ForwardInfo();
				replace(args, forwardInfo);
			}
			forwardInfo.setType(ForwardType.MOVING);
			Method method = ObjectUtils.getMethod(targetMethodName, MetaServer.class);
			return method.invoke(exportServer, args);
		}
		
		return joinpoint.proceed();
	}
	

	private void replace(Object[] args, ForwardInfo forwardInfo) {
		
		for(int i=0;i<args.length;i++){
			if(args[i] instanceof ForwardInfo){
				args[i] = forwardInfo;
				return;
			}
		}
		
		args[args.length - 1] = forwardInfo;
	}

	protected MetaServer exportServer(Object key){
		
		SlotInfo slotInfo = slotManager.getSlotInfoByKey(key);
		if(slotInfo.getSlotState() == SLOT_STATE.MOVING){
			MetaServer clusterServer = clusterServers.getClusterServer(slotInfo.getToServerId()); 
			logger.info("[exportServer]{}, {}", key, clusterServer);
			return clusterServer;
		}
		return null;
	}

}
