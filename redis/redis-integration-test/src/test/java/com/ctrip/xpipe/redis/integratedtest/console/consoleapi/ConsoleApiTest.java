package com.ctrip.xpipe.redis.integratedtest.console.consoleapi;

import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.integratedtest.console.consoleapi.util.ApiTestExecitorPool;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

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
		new ApiTestExecitorPool("DcMetaTest", p.getProperty("DcMetaTest"),
				DcMeta.class, 100000).doTest();
	}
}
