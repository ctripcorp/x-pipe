package com.ctrip.xpipe.redis.integratedtest.consoleapi;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.integratedtest.consoleapi.util.ApiTestExecitorPool;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author liuyi
 * 
 *         Sep 9, 2016
 */
public class ConsoleApiTest extends AbstractTest {
	private final static Logger logger = LoggerFactory
			.getLogger(ConsoleApiTest.class);
	private static Properties p = new Properties();

	@Before
	public void loadProperties() {
		try {
			p.load(new FileInputStream("/opt/data/100004374/console.properties"));
		} catch (FileNotFoundException e) {
			logger.error(
					"[loadProperties] /opt/data/100004374/console.properties not found",
					e);
		} catch (IOException e) {
			logger.error("[loadProperties]", e);
		}
	}

	@Test
	public void apiTest() {
		// apiNames=apiName1,apiName2,apiName3
		String[] apiNames = StringUtil.isEmpty(p.getProperty("apiNames")) ? new String[] {}
				: p.getProperty("apiNames").split(",");
		List<ApiTestExecitorPool> apiTests = new ArrayList<ApiTestExecitorPool>();
		for (String apiName : apiNames) {
			// apiName=url
			apiTests.add(new ApiTestExecitorPool(apiName, p
					.getProperty(apiName), ClusterMeta.class));
		}
		for (ApiTestExecitorPool apiTest : apiTests) {
			apiTest.doTest(1, 10, 100);
		}
		int successNum = 0;
		while (true) {
			synchronized (ApiTestExecitorPool.WAIT_OR_NOTIFY_LOCK) {
				try {
					ApiTestExecitorPool.WAIT_OR_NOTIFY_LOCK.wait();
				} catch (InterruptedException e) {
					logger.error("InterruptedException",e);
				}
			}
			for (ApiTestExecitorPool apiTest : apiTests) {
				if (apiTest.isOver) {
					if (apiTest.isPass) {
						successNum++;
					} else {
						apiTest.printErrorMessages();
						logger.error("{} did not pass the test",
								apiTest.getApiName());
						throw new RuntimeException(String.format(
								"%s did not pass the test",
								apiTest.getApiName()));
					}
				}
			}
			if (apiTests.size() == successNum) {
				break;
			}
			successNum = 0;
		}
	}
}
