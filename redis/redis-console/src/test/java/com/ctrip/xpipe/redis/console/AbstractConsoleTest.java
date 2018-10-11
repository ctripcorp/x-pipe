package com.ctrip.xpipe.redis.console;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig;
import com.ctrip.xpipe.redis.console.healthcheck.HealthChecker;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.config.DefaultHealthCheckConfig;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import org.junit.BeforeClass;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public abstract class AbstractConsoleTest extends AbstractRedisTest{
	
	@BeforeClass
	public static void beforeAbstractConsoleTest(){
		System.setProperty(HealthChecker.ENABLED, "false");
	}
	
	protected RedisHealthCheckInstance newRandomRedisHealthCheckInstance(String activeDc, int port) throws Exception {
		RedisMeta redisMeta = newRandomFakeRedisMeta().setPort(port);
		DefaultRedisInstanceInfo info = new DefaultRedisInstanceInfo(redisMeta.parent().parent().parent().getId(),
				redisMeta.parent().parent().getId(), redisMeta.parent().getId(),
				new HostPort(redisMeta.getIp(), redisMeta.getPort()),
				activeDc);
		return newRandomRedisHealthCheckInstance(info);
	}

	protected RedisHealthCheckInstance newRandomRedisHealthCheckInstance(int port) throws Exception {
		RedisMeta redisMeta = newRandomFakeRedisMeta().setPort(port);
		DefaultRedisInstanceInfo info = new DefaultRedisInstanceInfo(redisMeta.parent().parent().parent().getId(),
				redisMeta.parent().parent().getId(), redisMeta.parent().getId(),
				new HostPort(redisMeta.getIp(), redisMeta.getPort()),
				redisMeta.parent().getActiveDc());
		return newRandomRedisHealthCheckInstance(info);
	}

	protected RedisHealthCheckInstance newRandomRedisHealthCheckInstance(RedisInstanceInfo info) throws Exception {
		DefaultRedisHealthCheckInstance instance = new DefaultRedisHealthCheckInstance();
		instance.setRedisInstanceInfo(info);
		instance.setEndpoint(new DefaultEndPoint(info.getHostPort().getHost(), info.getHostPort().getPort()));
		instance.setHealthCheckConfig(new DefaultHealthCheckConfig(new DefaultConsoleConfig()));
		instance.setSession(new RedisSession(instance.getEndpoint(), scheduled, getXpipeNettyClientKeyedObjectPool()));
		return instance;
	}
}
