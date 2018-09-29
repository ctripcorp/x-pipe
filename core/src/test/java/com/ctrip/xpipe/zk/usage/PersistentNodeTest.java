package com.ctrip.xpipe.zk.usage;

import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

import java.io.IOException;


/**
 * @author wenchao.meng
 *
 * Aug 30, 2016
 */
public class PersistentNodeTest extends AbstractZkUsageTest{
	
	@Test
	public void testNode() throws IOException{
		
		String path = "/" + getTestName();
		PersistentNode persistentNode = new PersistentNode(client, CreateMode.EPHEMERAL, false, path, "123456".getBytes());
		persistentNode.start();
		
		waitForAnyKeyToExit();
		
		persistentNode.close();
		
	}
	
	

}
