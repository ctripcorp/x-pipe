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
 * 生产上由 {@link Production}（production profile）装配，依赖 ZkClient；测试上下文没有 ZK，用内存实现顶上。
 * checker 测试上下文的同类 bean 在 {@code AbstractCheckerIntegrationTest} 里，不进 console 测试上下文。
 */
@Configuration
@Profile(AbstractProfile.PROFILE_NAME_TEST)
public class TestKeeperContainerAddressConfig {

    @Bean
    public CurrentDcKeeperContainerAddressService keeperContainerAddressService() {
        return new TestCurrentDcKeeperContainerAddressService();
    }
}
