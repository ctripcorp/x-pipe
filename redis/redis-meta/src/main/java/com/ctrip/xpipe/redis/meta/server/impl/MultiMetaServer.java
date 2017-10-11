package com.ctrip.xpipe.redis.meta.server.impl;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfo;
import com.google.common.util.concurrent.MoreExecutors;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * @author wenchao.meng
 *
 *         Nov 30, 2016
 */
public class MultiMetaServer implements InvocationHandler {

	public static MetaServer newProxy(MetaServer dstServer, List<MetaServer> otherServers) {

		return (MetaServer) Proxy.newProxyInstance(MultiMetaServer.class.getClassLoader(),
				new Class[] { MetaServer.class }, new MultiMetaServer(dstServer, otherServers));

	}

	private MetaServer dstServer;
	private List<MetaServer> otherServers;
	private Executor executors;

	public MultiMetaServer(MetaServer dstServer, List<MetaServer> otherServers) {
		this(dstServer, otherServers, MoreExecutors.directExecutor());
	}

	public MultiMetaServer(MetaServer dstServer, List<MetaServer> otherServers, Executor executors) {
		this.dstServer = dstServer;
		this.otherServers = otherServers;
		this.executors = executors;
	}

	@Override
	public Object invoke(Object proxy, final Method method, final Object[] rawArgs) throws Throwable {

		for (final MetaServer metaServer : otherServers) {

			final Object[] args = copy(rawArgs);

			executors.execute(new AbstractExceptionLogTask() {

				@Override
				protected void doRun() throws Exception {
					method.invoke(metaServer, args);
				}
			});
		}
		final Object[] args = copy(rawArgs);
		return method.invoke(dstServer, args);
	}

	private Object[] copy(Object[] rawArgs) {

		Object[] array = Arrays.copyOf(rawArgs, rawArgs.length);
		for (int i = 0; i < array.length; i++) {

			if (array[i] instanceof ForwardInfo) {
				ForwardInfo rawForwardInfo = (ForwardInfo) array[i];
				array[i] = rawForwardInfo.clone();
			}
		}
		return array;
	}
}
