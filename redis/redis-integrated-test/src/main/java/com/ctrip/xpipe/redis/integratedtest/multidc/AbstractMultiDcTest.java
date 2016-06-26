package com.ctrip.xpipe.redis.integratedtest.multidc;





import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;

import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.integratedtest.AbstractIntegratedTestTemplate;

/**
 * @author wenchao.meng
 *
 * Jun 15, 2016
 */
public abstract class AbstractMultiDcTest extends AbstractIntegratedTestTemplate{
	

	@Before
	public void beforeAbstractMultiDcTest() throws Exception{

		if(startAllDc()){
			startXpipe();
//			updateUpstreamKeeper();
			sleep(6000);
		}
	}
	
	protected boolean startAllDc() {
		return true;
	}

	@Override
	protected List<RedisMeta> getRedisSlaves() {
		
		List<RedisMeta> result = new LinkedList<>();
		for(DcMeta dcMeta : getDcMetas()){
			List<RedisMeta> slaves = getRedisSlaves(dcMeta.getId());
			Assert.assertTrue(slaves.size() >= 1);
			result.addAll(slaves);
		}
		Assert.assertTrue(result.size() >= 1);
		return result;
	}
}
