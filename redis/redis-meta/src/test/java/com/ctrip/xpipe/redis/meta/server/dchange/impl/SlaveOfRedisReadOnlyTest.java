package com.ctrip.xpipe.redis.meta.server.dchange.impl;

import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.SlaveOfRedisReadOnly;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;

/**
 * @author wenchao.meng
 *
 *         Feb 24, 2017
 */
public class SlaveOfRedisReadOnlyTest extends AbstractMetaServerTest {

	private String host = "127.0.0.1";
	private int port = 6379;

	@Test
	public void test() throws Exception {

		Jedis jedis = createJedis(host, port);
		SlaveOfRedisReadOnly readOnly = new SlaveOfRedisReadOnly(host, port, getXpipeNettyClientKeyedObjectPool(),
				scheduled);

		for (int i = 0; i < 10; i++) {
			readOnly.makeReadOnly();

			try {
				jedis.set("a", "b");
				Assert.fail();
			} catch (Exception e) {
			}

			readOnly.makeWritable();
			jedis.set("a", "b");
		}
	}

}
