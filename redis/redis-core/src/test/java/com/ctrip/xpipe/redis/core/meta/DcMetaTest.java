package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import org.junit.Test;

/**
 * @author wenchao.meng
 *
 *         Oct 17, 2016
 */
public class DcMetaTest extends AbstractRedisTest {

	@Test
	public void test() {

		DcMeta dcMeta = getDcMeta("jq");
		Codec codec = new JsonCodec(true);
		String dcMetaStr = codec.encode(dcMeta);
		logger.info("{}", dcMetaStr);
		DcMeta dcMetaDe = codec.decode(dcMetaStr, DcMeta.class);

		logger.info("[test]{}", dcMeta.getClusters().get("cluster1").parent());
		logger.info("[test]{}", dcMetaDe.getClusters().get("cluster1").parent());
	}


	@Test
	public void testKeeper(){

		KeeperMeta keeperMeta = new KeeperMeta().setIp("127.0.0.1").setPort(6379);

		Codec codec = new JsonCodec(true);
		String keeperMetaDesc = codec.encode(keeperMeta);
		logger.info("{}", keeperMetaDesc);
		KeeperMeta keeperMetaDec = codec.decode(keeperMetaDesc, KeeperMeta.class);

		logger.info("{}", keeperMetaDec);
	}

	@Override
	protected String getXpipeMetaConfigFile() {
		return "dc-meta-test.xml";
	}
}
