package com.ctrip.xpipe.redis.keeper.spring;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.core.spring.AbstractWebConfig;
import com.ctrip.xpipe.redis.keeper.container.HostNameVerifyFilter;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author lishanglin
 * date 2021/5/18
 */
@Configuration
public class KeeperWebConfig extends AbstractWebConfig {

    /**
     * 校验直连命令是否打对了本容器。只拦管理面路径：KeeperContainerController 统一挂在
     * /keepers 下，addKeeper、removeKeeper、IO 限流、resetKeeper 等写操作都在其中。
     */
    @Bean
    public FilterRegistrationBean<HostNameVerifyFilter> hostNameVerifyFilter() {
        FilterRegistrationBean<HostNameVerifyFilter> registrationBean = new FilterRegistrationBean<>();
        registrationBean.setFilter(new HostNameVerifyFilter(
                () -> FoundationService.DEFAULT.getHostName(),
                () -> FoundationService.DEFAULT.getLocalIp()));
        // 显式列出 /keepers 自身：addKeeper（POST /keepers）与 list（GET /keepers）直接映射在
        // 该路径上，而 addKeeper 正是要防误伤的写操作。Servlet 的 /keepers/* 按规范也覆盖无斜杠
        // 的 /keepers，但这层保护不值得依赖容器实现细节。
        registrationBean.addUrlPatterns("/keepers", "/keepers/*");
        return registrationBean;
    }
}
