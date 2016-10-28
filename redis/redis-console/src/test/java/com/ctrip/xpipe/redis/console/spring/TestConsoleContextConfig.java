package com.ctrip.xpipe.redis.console.spring;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.TestConsoleConfig;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.console.service.notifier.ClusterMetaModifiedNotifier;
import com.ctrip.xpipe.redis.console.service.notifier.DefaultClusterMetaModifiedNotifier;
import com.ctrip.xpipe.redis.console.service.vo.DcMetaQueryVO;
import com.ctrip.xpipe.redis.console.util.DefaultMetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.console.util.MetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleServiceManager;
import com.ctrip.xpipe.redis.core.metaserver.impl.DefaultMetaServerConsoleServiceManager;

/**
 * @author shyin
 *
 * Oct 28, 2016
 */
@Configuration
@Import(com.ctrip.xpipe.redis.console.spring.TestConsoleMetaController.class)
public class TestConsoleContextConfig {
	@Bean
	public ConsoleConfig getConsoleConfig() {
		return new TestConsoleConfig();
	}
	
	@Bean
	public MetaServerConsoleServiceManager getMetaServerConsoleServiceManager() {
		return new DefaultMetaServerConsoleServiceManager();
	}

	@Bean
	public MetaServerConsoleServiceManagerWrapper getMetaServerConsoleServiceManagerWraper() {
		return new DefaultMetaServerConsoleServiceManagerWrapper();
	}
	
	@Bean
	public ClusterMetaModifiedNotifier getClusterMetaModifiedNotifier() {
		return new DefaultClusterMetaModifiedNotifier();
	}
	
	@Bean
	public ClusterMetaService getClusterMetaService() {
		return new ClusterMetaService() {
			
			@Override
			public ClusterMeta loadClusterMeta(DcMeta dcMeta, ClusterTbl clusterTbl, DcMetaQueryVO dcMetaQueryVO) {
				return new ClusterMeta();
			}
			
			@Override
			public ClusterMeta getClusterMeta(String dcName, String clusterName) {
				return new ClusterMeta();
			}
		};
	}
}
