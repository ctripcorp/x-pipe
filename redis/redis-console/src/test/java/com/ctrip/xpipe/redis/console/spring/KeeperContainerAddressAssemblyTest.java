package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.core.keeper.container.CurrentDcKeeperContainerAddressService;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.zk.ZkClient;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * keepercontainer 地址服务与其 ZK client 的装配位置：
 * 定义在 {@link ConsoleContextConfig}（CONSOLE 模式），{@link ConsoleCheckerContextConfig}
 * 经继承获得（CONSOLE_CHECKER 模式）；CHECKER 模式的 {@code CheckerContextConfig} 不继承本类，
 * 天然不装配。{@link Production} 不再持有这两个 Bean。
 */
public class KeeperContainerAddressAssemblyTest {

    @Test
    public void beansDeclaredInConsoleContextConfig() throws Exception {
        Method zkClient = ConsoleContextConfig.class.getDeclaredMethod("keeperContainerZkClient", ConsoleConfig.class);
        Method addressService = ConsoleContextConfig.class.getDeclaredMethod("keeperContainerAddressService",
                ConsoleConfig.class, ZkClient.class);

        Assert.assertEquals(ConsoleContextConfig.KEEPER_CONTAINER_ZK_CLIENT,
                zkClient.getAnnotation(Bean.class).value()[0]);
        Assert.assertEquals(CurrentDcKeeperContainerAddressService.class, addressService.getReturnType());

        // 与迁移前一致：仅 production profile 装配，测试上下文由 TestKeeperContainerAddressConfig 顶上
        for (Method method : Arrays.asList(zkClient, addressService)) {
            Profile profile = method.getAnnotation(Profile.class);
            Assert.assertNotNull(method.getName() + " 应限定 production profile", profile);
            Assert.assertTrue(Arrays.asList(profile.value()).contains(AbstractProfile.PROFILE_NAME_PRODUCTION));
        }
    }

    @Test
    public void beansInheritedByConsoleCheckerContextConfig() throws Exception {
        // getMethod 含继承：CONSOLE_CHECKER 模式的配置类处理父类 @Bean 方法，无需重复定义
        Assert.assertNotNull(ConsoleCheckerContextConfig.class.getMethod("keeperContainerZkClient", ConsoleConfig.class));
        Assert.assertNotNull(ConsoleCheckerContextConfig.class.getMethod("keeperContainerAddressService",
                ConsoleConfig.class, ZkClient.class));
    }

    @Test
    public void beansAbsentInCheckerMode() {
        // CheckerContextConfig 不继承 ConsoleContextConfig，getMethods（含继承）也不应有这两个 Bean 方法
        boolean found = Arrays.stream(CheckerContextConfig.class.getMethods())
                .anyMatch(m -> m.getName().equals("keeperContainerZkClient")
                        || m.getName().equals("keeperContainerAddressService"));
        Assert.assertFalse("CHECKER 模式不得装配 keepercontainer 地址服务与 ZK client", found);
    }

    @Test
    public void beansRemovedFromProduction() {
        boolean found = Arrays.stream(Production.class.getDeclaredMethods())
                .anyMatch(m -> m.getName().equals("getKeeperContainerZkClient")
                        || m.getName().equals("keeperContainerAddressService"));
        Assert.assertFalse("Production 不再持有 keepercontainer 地址服务与 ZK client", found);
    }

}
