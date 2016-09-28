package com.ctrip.xpipe.simple;

import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.springframework.web.client.RestOperations;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.spring.RestTemplateFactory;

/**
 * @author shyin
 *
 * Sep 28, 2016
 */
public class LegacyTest extends AbstractTest{
	@Test
	public void useHeapMemoryTest() throws Exception {
		List<RestOperations> storage = new LinkedList<>();
		while(true) {
			storage.add(RestTemplateFactory.createCommonsHttpRestTemplate());
		}
	}
	
	@Test
	public void usePermAreaTest() {
		String test = "";
		while(true) {
			test += "a";
		}
	}
}
