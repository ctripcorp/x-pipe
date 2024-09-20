package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.config.impl.CheckConfigBean;
import com.ctrip.xpipe.redis.checker.config.impl.CommonConfigBean;
import com.ctrip.xpipe.redis.checker.config.impl.ConsoleConfigBean;
import com.ctrip.xpipe.redis.checker.config.impl.DataCenterConfigBean;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig;
import com.ctrip.xpipe.redis.console.util.DefaultMetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.console.util.MetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleServiceManager;
import com.ctrip.xpipe.redis.core.metaserver.impl.DefaultMetaServerConsoleServiceManager;
import org.springframework.context.annotation.Bean;

/**
 * @author marsqing
 *
 *         Dec 1, 2016 1:27:57 PM
 */
//@Configuration
//@EnableAspectJAutoProxy
//@ComponentScan(basePackages = { //
//		"com.ctrip.xpipe.redis.console.service", //
//		"com.ctrip.xpipe.redis.console.dao", //
//		"com.ctrip.xpipe.redis.console.notifier" }, //
//		excludeFilters = @ComponentScan.Filter( //
//				type = FilterType.REGEX, //
//				pattern = "com.ctrip.xpipe.redis.console.controller"))
public class ContextConfig {

	@Bean
	public MetaServerConsoleServiceManager getMetaServerConsoleServiceManager() {
		return new DefaultMetaServerConsoleServiceManager();
	}

	@Bean
	public MetaServerConsoleServiceManagerWrapper getMetaServerConsoleServiceManagerWraper(ConsoleConfig config) {
		return new DefaultMetaServerConsoleServiceManagerWrapper(config);
	}

	@Bean
	public ConsoleConfig consoleConfig() {
		return new DefaultConsoleConfig(new CheckConfigBean(FoundationService.DEFAULT),
				new ConsoleConfigBean(FoundationService.DEFAULT),
				new DataCenterConfigBean(),
				new CommonConfigBean());
	}

}
