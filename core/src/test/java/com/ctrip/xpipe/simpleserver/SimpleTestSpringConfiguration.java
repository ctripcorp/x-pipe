package com.ctrip.xpipe.simpleserver;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author shyin
 *
 * Sep 20, 2016
 */
@RestController
public class SimpleTestSpringConfiguration {
	@Value("${target-response}")
	private String response;
	
	@RequestMapping(value = "/test", method=RequestMethod.GET)
	public String forTest() {
		return response;
	}
}
