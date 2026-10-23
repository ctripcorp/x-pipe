package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.redis.console.service.CrossDcKeeperContainerAddressService;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;

import java.lang.reflect.Field;

/**
 * {@code DefaultMetaCache} 注入 {@link CrossDcKeeperContainerAddressService} 会形成依赖环：
 * 地址服务 → RemoteDcKeeperContainerAddressCache → ConsoleServiceManager → MetaCache → 地址服务。
 * 该字段必须保持 {@code @Lazy}，否则 Spring 启动期直接报
 * {@code BeanCurrentlyInCreationException}（生产表现为 APPLICATION FAILED TO START）。
 */
public class MetaCacheAddressServiceCycleTest {

    @Test
    public void testAddressServiceFieldIsLazy() throws Exception {
        Field field = DefaultMetaCache.class.getDeclaredField("keeperContainerAddressService");

        Assert.assertNotNull("该字段参与依赖环，必须 @Lazy 注入以打破环", field.getAnnotation(Lazy.class));
        Assert.assertNotNull(field.getAnnotation(Autowired.class));
        Assert.assertFalse("地址服务缺失时应降级而非启动失败", field.getAnnotation(Autowired.class).required());
    }

    /**
     * 环里真正不可延后的一环是 {@code ConsoleServiceManager} 的构造函数注入；本类注入点若是
     * 立即解析的字段，Spring 必须在创建 MetaCache 时先把地址服务整条链建完，于是撞回自己。
     * 同为该环成员的 {@code consoleServiceManager} 字段也是 {@code @Lazy}，口径须一致。
     */
    @Test
    public void testConsoleServiceManagerFieldAlsoLazy() throws Exception {
        Field field = DefaultMetaCache.class.getDeclaredField("consoleServiceManager");

        Assert.assertNotNull("同一依赖环的既有成员，口径须与地址服务一致",
                field.getAnnotation(Lazy.class));
    }
}
