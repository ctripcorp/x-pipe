package com.ctrip.xpipe.redis.console.service;


import com.ctrip.xpipe.redis.console.controller.api.data.meta.RedisConfigCheckRuleCreateInfo;
import com.ctrip.xpipe.redis.console.model.RedisConfigCheckRuleTbl;

import java.util.List;

public interface RedisConfigCheckRuleService {

    void addRedisConfigCHeckRule(RedisConfigCheckRuleCreateInfo redisConfigCheckRuleCreateInfo);

    void updateRedisConfigCHeckRule(RedisConfigCheckRuleCreateInfo redisConfigCheckRuleCreateInfo);

    void deleteRedisConfigCheckRuleById(Long ruleId);

    void deleteRedisConfigCheckRuleByParam(String param);

    RedisConfigCheckRuleTbl getRedisConifgCheckRuleById(Long ruleId);

    RedisConfigCheckRuleTbl getRedisConifgCheckRuleByParam(String param);

    List<RedisConfigCheckRuleTbl> getRedisConifgCheckRulesByCheckType(String checkType);

    List<RedisConfigCheckRuleTbl> getAllRedisConfigCheckRules();

    List<RedisConfigCheckRuleCreateInfo> getRedisConifgCheckRuleInfosByCheckType(String checkType);

    List<RedisConfigCheckRuleCreateInfo> getAllRedisConfigCheckRuleInfos();
}
