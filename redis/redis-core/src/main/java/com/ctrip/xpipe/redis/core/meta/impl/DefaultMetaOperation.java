package com.ctrip.xpipe.redis.core.meta.impl;

import org.apache.curator.framework.CuratorFramework;

import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaOperation;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public class DefaultMetaOperation implements MetaOperation{
	
	private CuratorFramework curatorFramework;
	
	public DefaultMetaOperation(CuratorFramework curatorFramework) {
		this.curatorFramework = curatorFramework;
	}

	@Override
	public void update(String meta) throws Exception {
		
		curatorFramework.setData().forPath(MetaZkConfig.getMetaRootPath(), meta.getBytes());
	}

	@Override
	public void update(XpipeMeta xpipeMeta) throws Exception {
		
		curatorFramework.setData().forPath(MetaZkConfig.getMetaRootPath(), xpipeMeta.toString().getBytes());
	}

}
