package com.ctrip.xpipe.zk;

import org.apache.curator.framework.CuratorFramework;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.zk.impl.DefaultZkConfig;

/**
 * @author wenchao.meng
 *
 * Aug 29, 2016
 */
public class EnsurePathTest extends AbstractTest{
	
	int count = 100;
	
	@Test
	public void testEnsurePath() throws Exception{
		
		ZkConfig zkConfig = new DefaultZkConfig();
		CuratorFramework client = zkConfig.create("10.2.38.87");
		String path = "/" + getTestName();
		
		long begin = System.currentTimeMillis();
		
		for(int i=0;i<count;i++){
//			client.newNamespaceAwareEnsurePath("/" + getTestName()).ensure(client.getZookeeperClient());;
			client.createContainers(path);
		}
		
		long end = System.currentTimeMillis();
		
		System.out.println("time used:" + (end - begin));
		
	}
	

}
