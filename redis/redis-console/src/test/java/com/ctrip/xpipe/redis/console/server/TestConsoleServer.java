package com.ctrip.xpipe.redis.console.server;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Import;

/**
 * @author shyin
 *
 * Oct 28, 2016
 */
@EnableAutoConfiguration
@Import(com.ctrip.xpipe.redis.console.spring.TestConsoleContextConfig.class)
public class TestConsoleServer {

}
