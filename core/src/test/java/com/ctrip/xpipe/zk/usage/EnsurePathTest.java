package com.ctrip.xpipe.zk.usage;

import org.junit.Test;


/**
 * @author wenchao.meng
 *
 * Aug 29, 2016
 */
public class EnsurePathTest extends AbstractZkUsageTest{
	
	int count = 100;
	
	@Test
	public void testEnsurePath() throws Exception{
		
		String path = "/" + getTestName();
		
		long begin = System.currentTimeMillis();
		
		for(int i=0;i<count;i++){
//			client.newNamespaceAwareEnsurePath("/" + getTestName()).ensure(client.getZookeeperClient());
			client.createContainers(path);
		}
		
		long end = System.currentTimeMillis();
		
		System.out.println("time used:" + (end - begin));
		
	}
	

}
