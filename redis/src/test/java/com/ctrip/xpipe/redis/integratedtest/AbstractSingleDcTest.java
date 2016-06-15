package com.ctrip.xpipe.redis.integratedtest;

import java.awt.IllegalComponentStateException;

import org.junit.Before;

import com.ctrip.xpipe.redis.keeper.entity.ClusterMeta;
import com.ctrip.xpipe.redis.keeper.entity.DcMeta;
import com.ctrip.xpipe.redis.keeper.entity.RedisMeta;
import com.ctrip.xpipe.redis.keeper.entity.ShardMeta;

/**
 * @author wenchao.meng
 *
 * Jun 15, 2016
 */
public class AbstractSingleDcTest extends AbstractIntegratedTest{
	
	private DcMeta dcMeta;
	
	@Before
	public void beforeAbstractSingleDcTest() throws Exception{

		
		for(DcMeta dcMeta : getXpipeMeta().getDcs().values()){
			if(this.dcMeta != null){
				break;
			}
			for(ClusterMeta clusterMeta : dcMeta.getClusters().values()){
				if(this.dcMeta != null){
					break;
				}
				for(ShardMeta shardMeta : clusterMeta.getShards().values()){
					if(this.dcMeta != null){
						break;
					}
					for(RedisMeta redisMeta : shardMeta.getRedises()){
						if(redisMeta.getMaster()){
							this.dcMeta = dcMeta;
							break;
						}
					}
				}
			}
		}
		
		if(dcMeta == null){
			throw new IllegalComponentStateException("can not find dc with a active redis master");
		}
		
		startDc(dcMeta.getId());
	}
	
	public DcMeta getDcMeta() {
		return dcMeta;
	}
}
