package com.ctrip.xpipe.redis.integratedtest.full.singledc;

import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.integratedtest.DcInfo;
import com.ctrip.xpipe.redis.integratedtest.full.AbstractIntegratedTestTemplate;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import org.junit.Assert;
import org.junit.Before;

import java.awt.IllegalComponentStateException;
import java.util.List;



/**
 * @author wenchao.meng
 *
 * Jun 15, 2016
 */
public class AbstractSingleDcTest extends AbstractIntegratedTestTemplate{
	
	private DcMeta dcMeta;
	
	@Before
	public void beforeAbstractSingleDcTest() throws Exception{


		this.dcMeta = activeDc();
		
		if(dcMeta == null){
			throw new IllegalComponentStateException("can not find dc with a active redis master");
		}
		
		startDc(dcMeta.getId());
		sleep(7000);
	}
	
	public DcMeta getDcMeta() {
		return dcMeta;
	}
	
	protected List<RedisMeta> getRedises(){
		
		return getRedises(dcMeta.getId());
	}
	
	protected List<RedisMeta> getRedisSlaves(){
		
		List<RedisMeta> result = getRedisSlaves(dcMeta.getId());
		Assert.assertTrue(result.size() >= 1);
		return result;
	}
	
	public RedisKeeperServer getRedisKeeperServerActive() {
		return super.getRedisKeeperServerActive(dcMeta.getId());
	}
	
	protected DcInfo getDcInfo(){
		
		return getDcInfos().get(dcMeta.getId());
	}
	

}
