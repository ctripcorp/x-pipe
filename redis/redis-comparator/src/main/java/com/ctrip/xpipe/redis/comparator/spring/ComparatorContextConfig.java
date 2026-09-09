package com.ctrip.xpipe.redis.comparator.spring;

import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;

/**
 * 运行时基座：继承 {@link AbstractSpringConfigContext} 得到 {@code SCHEDULED_EXECUTOR} /
 * {@code GLOBAL_EXECUTOR}；额外注册 Netty 客户端池。不注册比对线程池（每分片专属线程，D33 ②）。
 */
@Configuration
@ComponentScan(
        basePackages = "com.ctrip.xpipe.redis.comparator",
        excludeFilters = @ComponentScan.Filter(
                type = FilterType.ANNOTATION,
                classes = SpringBootApplication.class
        )
)
public class ComparatorContextConfig extends AbstractSpringConfigContext {

    public static final String CLIENT_POOL = "clientPool";

    @Bean(name = CLIENT_POOL)
    public XpipeNettyClientKeyedObjectPool getClientPool() {
        return new XpipeNettyClientKeyedObjectPool();
    }
}
