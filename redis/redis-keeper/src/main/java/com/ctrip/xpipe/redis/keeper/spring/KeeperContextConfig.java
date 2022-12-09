package com.ctrip.xpipe.redis.keeper.spring;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.cluster.DefaultLeaderElectorManager;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.GeneralRedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserFactory;
import com.ctrip.xpipe.redis.core.spring.AbstractRedisConfigContext;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.zk.ZkClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;


/**
 * @author marsqing
 *
 *         May 26, 2016 6:23:55 PM
 */
@Configuration
@ComponentScan({"com.ctrip.xpipe.redis.keeper", "com.ctrip.xpipe.redis.core.redis.operation.parser" })
public class KeeperContextConfig extends AbstractRedisConfigContext{
		
	@Bean
	public LeaderElectorManager geElectorManager(KeeperConfig  keeperConfig, ZkClient zkClient){
		
		return new DefaultLeaderElectorManager(zkClient);
	}

	@Bean
	public GeneralRedisOpParser GeneralRedisOpParser (RedisOpParserManager redisOpParserManager) {

		RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
		return new GeneralRedisOpParser(redisOpParserManager);
	}
}
