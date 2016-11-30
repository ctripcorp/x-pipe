package com.ctrip.xpipe.redis.meta.server.impl;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfo;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * @author wenchao.meng
 *
 * Nov 30, 2016
 */
public class MultiMetaServer implements InvocationHandler{
	
	public static MetaServer newProxy(List<MetaServer> metaServers){
		
		return (MetaServer) Proxy.newProxyInstance(MultiMetaServer.class.getClassLoader(), new Class[]{MetaServer.class}, new MultiMetaServer(metaServers));
		
	}
	
	private List<MetaServer> metaServers;
	private ExecutorService executors;
	
	public MultiMetaServer(List<MetaServer> metaServers) {
		this(metaServers, MoreExecutors.sameThreadExecutor());
	}

	public MultiMetaServer(List<MetaServer> metaServers, ExecutorService executors) {
		this.metaServers = metaServers;
		this.executors = executors;
	}

	@Override
	public Object invoke(Object proxy, final Method method, final Object[] rawArgs) throws Throwable {
		
		ForwardInfo rawForwardInfo = null;
		int forwradIndex = -1;
		
		for(int i=0;i<rawArgs.length;i++){
			if(rawArgs[i] instanceof ForwardInfo){
				forwradIndex = i;
				rawForwardInfo = (ForwardInfo) rawArgs[i];
			}
			
		}
		for(final MetaServer currentMetaServer : metaServers){

			final Object[] args = copy(rawArgs);
			if(rawForwardInfo != null){
				args[forwradIndex] = rawForwardInfo.clone();
			}
			
			executors.execute(new AbstractExceptionLogTask() {
				
				
				@Override
				protected void doRun() throws Exception {
					
					method.invoke(currentMetaServer, args);
				}

			});
		}
		//if multi return null
		return null;
	}

	private Object[] copy(Object[] rawArgs) {
		
		return Arrays.copyOf(rawArgs, rawArgs.length);
	}
}
