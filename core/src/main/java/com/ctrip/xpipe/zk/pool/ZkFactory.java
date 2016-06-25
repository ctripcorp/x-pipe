package com.ctrip.xpipe.zk.pool;


import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.curator.framework.CuratorFramework;
import com.ctrip.xpipe.zk.ZkConfig;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public class ZkFactory implements KeyedPoolableObjectFactory<String, CuratorFramework>{
	
	private ZkConfig zkConfig;
	
	public ZkFactory(ZkConfig zkConfig) {
		this.zkConfig = zkConfig;
	}
	
	@Override
	public CuratorFramework makeObject(String key) throws Exception {

		return zkConfig.create(key);
	}

	@Override
	public void destroyObject(String key, CuratorFramework obj) throws Exception {
		obj.close();
		
	}

	@Override
	public boolean validateObject(String key, CuratorFramework obj) {
		return true;
	}

	@Override
	public void activateObject(String key, CuratorFramework obj) throws Exception {
		
	}

	@Override
	public void passivateObject(String key, CuratorFramework obj) throws Exception {
		
	}

}
