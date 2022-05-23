package com.ctrip.xpipe.redis.console.service;


import com.ctrip.xpipe.redis.console.controller.api.data.meta.RedisCheckRuleCreateInfo;
import com.ctrip.xpipe.redis.console.model.RedisCheckRuleTbl;

import java.util.List;

public interface RedisCheckRuleService {

    void addRedisCheckRule(RedisCheckRuleCreateInfo redisCheckRuleCreateInfo);

    void updateRedisCheckRule(RedisCheckRuleCreateInfo redisCheckRuleCreateInfo);

    void deleteRedisCheckRuleById(Long ruleId);

    void deleteRedisCheckRuleByParam(String param);

    RedisCheckRuleTbl getRedisCheckRuleById(Long ruleId);

    RedisCheckRuleTbl getRedisCheckRuleByParam(String param);

    List<RedisCheckRuleTbl> getRedisCheckRulesByCheckType(String checkType);

    List<RedisCheckRuleTbl> getAllRedisCheckRules();

    List<RedisCheckRuleCreateInfo> getRedisCheckRuleInfosByCheckType(String checkType);

    List<RedisCheckRuleCreateInfo> getAllRedisCheckRuleInfos();
}
