package com.ctrip.xpipe.redis.integratedtest.consoleapi;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.integratedtest.consoleapi.util.ApiTestExecitorPool;

/**
 * @author liuyi
 * 
 *         Sep 9, 2016
 */
public class ConsoleApiTest {

	private static Properties p = new Properties();
	
	static {
		try {
			p.load(new FileInputStream("/opt/data/100004374/console.properties"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {

		new ApiTestExecitorPool("apiName1", p.getProperty("apiName1"),
				ClusterMeta.class).doTest();
		new ApiTestExecitorPool("apiName2",p.getProperty("apiName2"),
				ClusterMeta.class).doTest(1, 1000, 10);
	}
}
