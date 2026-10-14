package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.lifecycle.SpringComponentLifecycleManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.core.keeper.container.CurrentDcKeeperContainerAddressService;
import com.ctrip.xpipe.redis.core.keeper.container.DefaultCurrentDcKeeperContainerAddressService;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.zk.ZkClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 12, 2017
 */
@Configuration
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class Production extends AbstractProfile {

    public static final String KEEPER_CONTAINER_ZK_CLIENT = "keeperContainerZkClient";

    @Bean
    @Primary
    public ZkClient getZkClient(ConsoleConfig consoleConfig) {
        ZkClient zkClient = getZkClient(consoleConfig.getZkNameSpace(), consoleConfig.getZkConnectionString());
        consoleConfig.addListener(zkClient);
        return zkClient;
    }

    @Bean(KEEPER_CONTAINER_ZK_CLIENT)
    public ZkClient getKeeperContainerZkClient(ConsoleConfig consoleConfig) {
        ZkClient zkClient = getZkClient(consoleConfig.getKeeperContainerZkNameSpace(),
                consoleConfig.getZkConnectionString());
        consoleConfig.addListener(zkClient);
        return zkClient;
    }

    @Bean
    public SpringComponentLifecycleManager getSpringComponentLifecycleManager(){
        return new SpringComponentLifecycleManager();
    }

    @Bean
    public CurrentDcKeeperContainerAddressService keeperContainerAddressService(
            ConsoleConfig consoleConfig,
            @Qualifier(KEEPER_CONTAINER_ZK_CLIENT) ZkClient zkClient) {
        return new DefaultCurrentDcKeeperContainerAddressService(zkClient,
                consoleConfig::getKeeperContainerAddressMaxRemovedPercent,
                consoleConfig::getKeeperContainerAddressMinRemovedHostnames);
    }

}
