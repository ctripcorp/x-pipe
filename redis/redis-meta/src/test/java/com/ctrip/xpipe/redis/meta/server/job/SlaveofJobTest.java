package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.IpUtils;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

/**
 * @author wenchao.meng
 *
 *         Oct 28, 2016
 */
public class SlaveofJobTest extends AbstractMetaServerTest {

	private String[] redises = new String[] { "127.0.0.1:6379", "127.0.0.1:6479" };

	@Test
	public void test() throws Exception {

		SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool = getXpipeNettyClientKeyedObjectPool();

		List<RedisMeta> slaves = getRedisSlaves(redises);

		SlaveofJob slaveofJob = new SlaveofJob(slaves, "10.2.58.242", 6379, clientPool, scheduled, executors);
		slaveofJob.execute().get();

	}

	private List<RedisMeta> getRedisSlaves(String[] redises) {

		List<RedisMeta> slaves = new LinkedList<>();
		for (String redis : redises) {

			Pair<String, Integer> addr = IpUtils.parseSingleAsPair(redis);
			RedisMeta redisMeta = new RedisMeta();
			redisMeta.setIp(addr.getKey());
			redisMeta.setPort(addr.getValue());

			slaves.add(redisMeta);
		}

		return slaves;
	}

}
