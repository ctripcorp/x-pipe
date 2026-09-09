package com.ctrip.xpipe.redis.comparator.spring;

import com.ctrip.xpipe.spring.AbstractProfile;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * 生产 profile 下注册外部依赖 Bean。CMS {@code ServerGroupProvider} 实现落在 Phase LB；
 * 测试 profile 不加载本类，单测不访问网络（D23）。
 */
@Configuration
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class Production extends AbstractProfile {
}
