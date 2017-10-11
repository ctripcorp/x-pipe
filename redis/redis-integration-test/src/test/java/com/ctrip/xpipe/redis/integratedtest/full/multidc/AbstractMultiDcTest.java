package com.ctrip.xpipe.redis.integratedtest.full.multidc;


import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.integratedtest.full.AbstractIntegratedTestTemplate;
import org.junit.Before;

import java.util.List;

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
			sleep(6000);
		}
	}
	
	protected boolean startAllDc() {
		return true;
	}
	
	@Override
	protected List<RedisMeta> getRedisSlaves() {
		return getAllRedisSlaves();
	}

}
