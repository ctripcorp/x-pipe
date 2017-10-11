package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 *
 *         Dec 25, 2016
 */
public class LfReaderTest extends AbstractRedisTest {

	@Test
	public void test() {

		Assert.assertEquals(0, new LfReader().read(Unpooled.wrappedBuffer("\n".getBytes())).getPayload().length);

		
		String data = randomString(10) + "\r";

		Assert.assertNull(new LfReader().read(Unpooled.wrappedBuffer(data.getBytes())));

		String data1 = data + "\n";

		LfReader reader = (LfReader) new LfReader().read(Unpooled.wrappedBuffer(data1.getBytes()));
		Assert.assertNotNull(reader);
		Assert.assertEquals(data, new String(reader.getPayload()));
	}

}
