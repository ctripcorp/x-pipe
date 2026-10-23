package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.redis.core.keeper.container.CurrentDcKeeperContainerAddressService;
import com.ctrip.xpipe.redis.core.keeper.container.TestCurrentDcKeeperContainerAddressService;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * console 测试上下文的 {@link CurrentDcKeeperContainerAddressService}。
 * <p>
 * 生产上由 {@link ConsoleContextConfig}（CONSOLE / CONSOLE_CHECKER 经继承）装配，依赖 ZkClient；
 * 测试上下文没有 ZK，用内存实现顶上。CHECKER 模式不装配该 Bean。
 */
@Configuration
@Profile(AbstractProfile.PROFILE_NAME_TEST)
public class TestKeeperContainerAddressConfig {

    @Bean
    public CurrentDcKeeperContainerAddressService keeperContainerAddressService() {
        return new TestCurrentDcKeeperContainerAddressService();
    }
}
