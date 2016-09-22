package com.ctrip.xpipe.simpleserver;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author shyin
 *
 * Sep 20, 2016
 */
@RestController
public class SimpleTestSpringConfiguration {
	@RequestMapping("/test")
	public String forTest() {
		return "for test";
	}
}
