package com.ctrip.xpipe.redis.console.health.hickwall;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// code from https://liveramp.com/engineering/reconnecting-thrift-client/
public final class ReconnectingThriftClient {
	private static final Logger LOG = LoggerFactory.getLogger(ReconnectingThriftClient.class);

	/**
	 * List of causes which suggest a restart might fix things (defined as
	 * constants in {@link org.apache.thrift.transport.TTransportException}).
	 */
	private static final Set<Integer> RESTARTABLE_CAUSES = Sets.newHashSet(TTransportException.NOT_OPEN, TTransportException.END_OF_FILE,
			TTransportException.TIMED_OUT, TTransportException.UNKNOWN);

	public static class Options {
		private int numRetries;
		private long timeBetweenRetries;

		/**
		 *
		 * @param numRetries
		 *            the maximum number of times to try reconnecting before
		 *            giving up and throwing an exception
		 * @param timeBetweenRetries
		 *            the number of milliseconds to wait in between reconnection
		 *            attempts.
		 */
		public Options(int numRetries, long timeBetweenRetries) {
			this.numRetries = numRetries;
			this.timeBetweenRetries = timeBetweenRetries;
		}

		private int getNumRetries() {
			return numRetries;
		}

		private long getTimeBetweenRetries() {
			return timeBetweenRetries;
		}

		public Options withNumRetries(int numRetries) {
			this.numRetries = numRetries;
			return this;
		}

		public Options withTimeBetweenRetries(long timeBetweenRetries) {
			this.timeBetweenRetries = timeBetweenRetries;
			return this;
		}

		public static Options defaults() {
			return new Options(5, 5000L);
		}
	}

	/**
	 * Reflectively wraps a thrift client so that when a call fails due to a
	 * networking error, a reconnect is attempted.
	 *
	 * @param baseClient
	 *            the client to wrap
	 * @param clientInterface
	 *            the interface that the client implements (can be inferred by
	 *            using
	 *            {@link #wrap(org.apache.thrift.TServiceClient, com.rapleaf.spruce_lib.singletons.ReconnectingThriftClient.Options)}
	 * @param options
	 *            options that control behavior of the reconnecting client
	 * @param <T>
	 * @param <C>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T extends TServiceClient, C> C wrap(T baseClient, Class<C> clientInterface, Options options) {
		Object proxyObject = Proxy.newProxyInstance(clientInterface.getClassLoader(), new Class<?>[] { clientInterface },
				new ReconnectingClientProxy<T>(baseClient, options.getNumRetries(), options.getTimeBetweenRetries()));

		return (C) proxyObject;
	}

	/**
	 * Reflectively wraps a thrift client so that when a call fails due to a
	 * networking error, a reconnect is attempted.
	 *
	 * @param baseClient
	 *            the client to wrap
	 * @param options
	 *            options that control behavior of the reconnecting client
	 * @param <T>
	 * @param <C>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T extends TServiceClient, C> C wrap(T baseClient, Options options) {
		Class<?>[] interfaces = baseClient.getClass().getInterfaces();

		for (Class<?> iface : interfaces) {
			if (iface.getSimpleName().equals("Iface") && iface.getEnclosingClass().equals(baseClient.getClass().getEnclosingClass())) {
				return (C) wrap(baseClient, iface, options);
			}
		}

		throw new RuntimeException("Class needs to implement Iface directly. Use wrap(TServiceClient, Class) instead.");
	}

	/**
	 * Reflectively wraps a thrift client so that when a call fails due to a
	 * networking error, a reconnect is attempted.
	 *
	 * @param baseClient
	 *            the client to wrap
	 * @param clientInterface
	 *            the interface that the client implements (can be inferred by
	 *            using
	 *            {@link #wrap(org.apache.thrift.TServiceClient, com.rapleaf.spruce_lib.singletons.ReconnectingThriftClient.Options)}
	 * @param <T>
	 * @param <C>
	 * @return
	 */
	public static <T extends TServiceClient, C> C wrap(T baseClient, Class<C> clientInterface) {
		return wrap(baseClient, clientInterface, Options.defaults());
	}

	/**
	 * Reflectively wraps a thrift client so that when a call fails due to a
	 * networking error, a reconnect is attempted.
	 *
	 * @param baseClient
	 *            the client to wrap
	 * @param <T>
	 * @param <C>
	 * @return
	 */
	public static <T extends TServiceClient, C> C wrap(T baseClient) {
		return wrap(baseClient, Options.defaults());
	}

	/**
	 * Helper proxy class. Attempts to call method on proxy object wrapped in
	 * try/catch. If it fails, it attempts a reconnect and tries the method
	 * again.
	 *
	 * @param <T>
	 */
	private static class ReconnectingClientProxy<T extends TServiceClient> implements InvocationHandler {
		private final T baseClient;
		private final int maxRetries;
		private final long timeBetweenRetries;

		public ReconnectingClientProxy(T baseClient, int maxRetries, long timeBetweenRetries) {
			this.baseClient = baseClient;
			this.maxRetries = maxRetries;
			this.timeBetweenRetries = timeBetweenRetries;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			try {
				return method.invoke(baseClient, args);
			} catch (InvocationTargetException e) {
				if (e.getTargetException() instanceof TTransportException) {
					TTransportException cause = (TTransportException) e.getTargetException();

					if (RESTARTABLE_CAUSES.contains(cause.getType())) {
						reconnectOrThrowException(baseClient.getInputProtocol().getTransport(), maxRetries, timeBetweenRetries);
						return method.invoke(baseClient, args);
					}
				}

				throw e;
			}
		}

		private static void reconnectOrThrowException(TTransport transport, int maxRetries, long timeBetweenRetries) throws TTransportException {
			int errors = 0;
			transport.close();
			
			while (errors < maxRetries) {
				try {
					LOG.info("Attempting to reconnect...");
					transport.open();
					LOG.info("Reconnection successful");
					break;
				} catch (TTransportException e) {
					LOG.error("Error while reconnecting:", e);
					errors++;

					if (errors < maxRetries) {
						try {
							LOG.info("Sleeping for {} milliseconds before retrying", timeBetweenRetries);
							Thread.sleep(timeBetweenRetries);
						} catch (InterruptedException e2) {
							throw new RuntimeException(e);
						}
					}
				}
			}

			if (errors >= maxRetries) {
				throw new TTransportException("Failed to reconnect");
			}
		}
	}
}