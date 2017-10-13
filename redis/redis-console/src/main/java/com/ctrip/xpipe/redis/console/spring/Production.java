package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.impl.DefaultZkConfig;
import com.ctrip.xpipe.zk.impl.SpringZkClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 12, 2017
 */
@Configuration
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class Production extends AbstractProfile {

    @Bean
    public ZkClient getZkClient(ConsoleConfig consoleConfig) {
        return getZkClient(consoleConfig.getZkNameSpace(), consoleConfig.getZkConnectionString());
    }

    @Override
    protected ZkClient getZkClient(String zkNameSpace, String zkAddress) {

        DefaultZkConfig zkConfig = new DefaultZkConfig();
        zkConfig.setZkNameSpace(zkNameSpace);

        SpringZkClient springZkClient = new SpringZkClient(zkConfig, zkAddress);
        return springZkClient;
    }
}