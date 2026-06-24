package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cache.TimeBoundCache;
import com.ctrip.xpipe.redis.checker.spring.ConsoleDisableDbCondition;
import com.ctrip.xpipe.redis.checker.spring.DisableDbMode;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.RedisCheckRuleCreateInfo;
import com.ctrip.xpipe.redis.console.model.RedisCheckRuleTbl;
import com.ctrip.xpipe.redis.console.resources.ConsolePortalService;
import com.ctrip.xpipe.redis.console.service.RedisCheckRuleService;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Service
@Conditional(ConsoleDisableDbCondition.class)
@DisableDbMode(true)
public class RedisCheckRuleServiceWithoutDB implements RedisCheckRuleService {

    @Autowired
    private ConsolePortalService consolePortalService;

    @Autowired
    private ConsoleConfig config;

    private TimeBoundCache<List<RedisCheckRuleTbl>> allRules;

    @PostConstruct
    public void init() {
        allRules = new TimeBoundCache<>(config::getCacheRefreshInterval, this::loadAllRedisCheckRules);
    }

    private List<RedisCheckRuleTbl> loadAllRedisCheckRules() {
        List<RedisCheckRuleCreateInfo> infos = consolePortalService.getAllRedisCheckRuleInfos();
        if (infos == null) {
            return Collections.emptyList();
        }
        List<RedisCheckRuleTbl> result = new ArrayList<>(infos.size());
        for (RedisCheckRuleCreateInfo info : infos) {
            result.add(toTbl(info));
        }
        return result;
    }

    private RedisCheckRuleTbl toTbl(RedisCheckRuleCreateInfo info) {
        RedisCheckRuleTbl tbl = new RedisCheckRuleTbl();
        if (info.getId() != null) {
            tbl.setId(info.getId());
        }
        tbl.setCheckType(info.getCheckType());
        tbl.setParam(info.getParam());
        tbl.setDescription(info.getDescription());
        return tbl;
    }

    @Override
    public void addRedisCheckRule(RedisCheckRuleCreateInfo redisCheckRuleCreateInfo) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateRedisCheckRule(RedisCheckRuleCreateInfo redisCheckRuleCreateInfo) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteRedisCheckRuleById(Long ruleId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteRedisCheckRuleByParam(String param) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RedisCheckRuleTbl getRedisCheckRuleById(Long ruleId) {
        for (RedisCheckRuleTbl rule : allRules.getData()) {
            if (rule.getId() == ruleId) {
                return rule;
            }
        }
        return null;
    }

    @Override
    public RedisCheckRuleTbl getRedisCheckRuleByParam(String param) {
        for (RedisCheckRuleTbl rule : allRules.getData()) {
            if (StringUtil.trimEquals(rule.getParam(), param)) {
                return rule;
            }
        }
        return null;
    }

    @Override
    public List<RedisCheckRuleTbl> getRedisCheckRulesByCheckType(String checkType) {
        if (!"info".equals(checkType) && !"config".equals(checkType)) {
            throw new IllegalArgumentException("checkType must be config or info");
        }
        List<RedisCheckRuleTbl> result = new ArrayList<>();
        for (RedisCheckRuleTbl rule : allRules.getData()) {
            if (StringUtil.trimEquals(rule.getCheckType(), checkType)) {
                result.add(rule);
            }
        }
        return result;
    }

    @Override
    public List<RedisCheckRuleTbl> getAllRedisCheckRules() {
        return allRules.getData();
    }

    @Override
    public List<RedisCheckRuleCreateInfo> getRedisCheckRuleInfosByCheckType(String checkType) {
        return Lists.newArrayList(Lists.transform(getRedisCheckRulesByCheckType(checkType),
                new Function<RedisCheckRuleTbl, RedisCheckRuleCreateInfo>() {
                    @Override
                    public RedisCheckRuleCreateInfo apply(RedisCheckRuleTbl redisCheckRuleTbl) {
                        return new RedisCheckRuleCreateInfo()
                                .setId(redisCheckRuleTbl.getId())
                                .setCheckType(redisCheckRuleTbl.getCheckType())
                                .setParam(redisCheckRuleTbl.getParam())
                                .setDescription(redisCheckRuleTbl.getDescription());
                    }
                }));
    }

    @Override
    public List<RedisCheckRuleCreateInfo> getAllRedisCheckRuleInfos() {
        return Lists.newArrayList(Lists.transform(getAllRedisCheckRules(),
                new Function<RedisCheckRuleTbl, RedisCheckRuleCreateInfo>() {
                    @Override
                    public RedisCheckRuleCreateInfo apply(RedisCheckRuleTbl redisCheckRuleTbl) {
                        return new RedisCheckRuleCreateInfo()
                                .setId(redisCheckRuleTbl.getId())
                                .setCheckType(redisCheckRuleTbl.getCheckType())
                                .setParam(redisCheckRuleTbl.getParam())
                                .setDescription(redisCheckRuleTbl.getDescription());
                    }
                }));
    }
}
