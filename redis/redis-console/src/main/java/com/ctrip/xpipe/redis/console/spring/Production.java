package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.lifecycle.SpringComponentLifecycleManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.zk.ZkClient;
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
        ZkClient zkClient = getZkClient(consoleConfig.getZkNameSpace(), consoleConfig.getZkConnectionString());
        consoleConfig.addListener(zkClient);
        return zkClient;
    }

    @Bean
    public SpringComponentLifecycleManager getSpringComponentLifecycleManager(){
        return new SpringComponentLifecycleManager();
    }

}