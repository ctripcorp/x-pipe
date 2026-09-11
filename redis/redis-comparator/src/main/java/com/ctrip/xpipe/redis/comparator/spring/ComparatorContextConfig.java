package com.ctrip.xpipe.redis.comparator.spring;

import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;

/**
 * 运行时基座：继承 {@link AbstractSpringConfigContext} 得到 {@code SCHEDULED_EXECUTOR} /
 * {@code GLOBAL_EXECUTOR}。不注册共享 Netty keyed pool —— 每路流自管一根复制连接
 * （流构造注入 {@code EventLoop}，应用持有 {@code EventLoopGroup}，D32 / §4.8.1）。
 * 不注册比对线程池（每分片专属线程，D33 ②）。
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
}
