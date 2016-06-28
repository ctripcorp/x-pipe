package com.ctrip.xpipe.zk.pool;


import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.curator.framework.CuratorFramework;
import com.ctrip.xpipe.zk.ZkConfig;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public class ZkFactory implements KeyedPooledObjectFactory<String, CuratorFramework>{
	
	private ZkConfig zkConfig;
	
	public ZkFactory(ZkConfig zkConfig) {
		this.zkConfig = zkConfig;
	}
	
	@Override
	public PooledObject<CuratorFramework> makeObject(String key) throws Exception {
		
		CuratorFramework curatorFramework = zkConfig.create(key);
		return new DefaultPooledObject<>(curatorFramework);
	}

	@Override
	public void destroyObject(String key, PooledObject<CuratorFramework> p) throws Exception {
		
		CuratorFramework curatorFramework = p.getObject();
		if(curatorFramework != null){
			curatorFramework.close();
		}
	}

	@Override
	public boolean validateObject(String key, PooledObject<CuratorFramework> p) {
		return true;
	}

	@Override
	public void activateObject(String key, PooledObject<CuratorFramework> p) throws Exception {
		
	}

	@Override
	public void passivateObject(String key, PooledObject<CuratorFramework> p) throws Exception {
		
	}
}
