package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.controller.api.data.meta.CheckFailException;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.RedisConfigCheckRuleCreateInfo;
import com.ctrip.xpipe.redis.console.dao.RedisConfigCheckRuleDao;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.RedisConfigCheckRuleTbl;
import com.ctrip.xpipe.redis.console.model.RedisConfigCheckRuleTblDao;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.RedisConfigCheckRuleService;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class RedisConfigCheckRuleServiceImpl extends AbstractConsoleService<RedisConfigCheckRuleTblDao> implements RedisConfigCheckRuleService {

    @Autowired
    private RedisConfigCheckRuleDao redisConfigCheckRuleDao;

    @Override
    public void addRedisConfigCHeckRule(RedisConfigCheckRuleCreateInfo redisConfigCheckRuleCreateInfo) {
        RedisConfigCheckRuleTbl proto = dao.createLocal();

        if(redisConfigCheckRuleIsExist(redisConfigCheckRuleCreateInfo)){
            throw new IllegalArgumentException("Redis config check rule : " + redisConfigCheckRuleCreateInfo.getCheckType() + redisConfigCheckRuleCreateInfo.getParam() + " already exists");
        }

        if(redisConfigCheckRuleCreateInfo.getDescription() == null)
            proto.setDescription("");
        else
            proto.setDescription(redisConfigCheckRuleCreateInfo.getDescription());

        proto.setCheckType(redisConfigCheckRuleCreateInfo.getCheckType()).setParam(redisConfigCheckRuleCreateInfo.getParam());

        redisConfigCheckRuleDao.addRedisConfigCHeckRule(proto);
    }

    @Override
    public void updateRedisConfigCHeckRule(RedisConfigCheckRuleCreateInfo redisConfigCheckRuleCreateInfo) {
        RedisConfigCheckRuleTbl redisConfigCheckRuleTbl = getRedisConifgCheckRuleById(redisConfigCheckRuleCreateInfo.getId());

        if(null == redisConfigCheckRuleTbl)
            throw new IllegalArgumentException(String.format("Redis config check rule %s %s not found", redisConfigCheckRuleCreateInfo.getCheckType(), redisConfigCheckRuleCreateInfo.getParam()));

        if(redisConfigCheckRuleCreateInfo.getCheckType() != null)
            redisConfigCheckRuleTbl.setCheckType(redisConfigCheckRuleCreateInfo.getCheckType());
        if(redisConfigCheckRuleCreateInfo.getParam() != null)
            redisConfigCheckRuleTbl.setParam(redisConfigCheckRuleCreateInfo.getParam());
        if(redisConfigCheckRuleCreateInfo.getDescription() != null)
            redisConfigCheckRuleTbl.setDescription(redisConfigCheckRuleCreateInfo.getDescription());

        redisConfigCheckRuleDao.updateRedisConfigCHeckRule(redisConfigCheckRuleTbl);
    }

    @Override
    public void deleteRedisConfigCheckRuleById(Long ruleId) {
        RedisConfigCheckRuleTbl redisConfigCheckRuleTbl = getRedisConifgCheckRuleById(ruleId);
        if(null == redisConfigCheckRuleTbl)
            throw new BadRequestException(String.format("Redis config check rule %d not found",ruleId));

        RedisConfigCheckRuleTbl proto = redisConfigCheckRuleTbl;
        redisConfigCheckRuleDao.deleteRedisConfigCheckRule(proto);
    }

    @Override
    public void deleteRedisConfigCheckRuleByParam(String param) {
        RedisConfigCheckRuleTbl redisConfigCheckRuleTbl = getRedisConifgCheckRuleByParam(param);
        if(null == redisConfigCheckRuleTbl)
            throw new BadRequestException(String.format("Redis config check rule %s not found",param));

        RedisConfigCheckRuleTbl proto = redisConfigCheckRuleTbl;
        redisConfigCheckRuleDao.deleteRedisConfigCheckRule(proto);
    }


    @Override
    public RedisConfigCheckRuleTbl getRedisConifgCheckRuleById(Long ruleId) {
        return redisConfigCheckRuleDao.getRedisConifgCheckRuleById(ruleId);
    }

    @Override
    public RedisConfigCheckRuleTbl getRedisConifgCheckRuleByParam(String param) {
        return redisConfigCheckRuleDao.getRedisConifgCheckRuleByParam(param);
    }

    @Override
    public List<RedisConfigCheckRuleTbl> getRedisConifgCheckRulesByCheckType(String checkType) {
        if(!"info".equals(checkType) && !"config".equals(checkType)){
            throw new IllegalArgumentException("checkType must be config or info");
        }

        return redisConfigCheckRuleDao.getRedisConifgCheckRulesByCheckType(checkType);
    }

    @Override
    public List<RedisConfigCheckRuleTbl> getAllRedisConfigCheckRules() {
        return redisConfigCheckRuleDao.getAllRedisConfigCheckRules();
    }

    @Override
    public List<RedisConfigCheckRuleCreateInfo> getRedisConifgCheckRuleInfosByCheckType(String checkType) {
        return Lists.newArrayList(Lists.transform(getRedisConifgCheckRulesByCheckType(checkType), new Function<RedisConfigCheckRuleTbl, RedisConfigCheckRuleCreateInfo>() {
            @Override
            public RedisConfigCheckRuleCreateInfo apply(RedisConfigCheckRuleTbl redisConfigCheckRuleTbl) {
                RedisConfigCheckRuleCreateInfo redisConfigCheckRuleCreateInfo =
                        new RedisConfigCheckRuleCreateInfo().setId(redisConfigCheckRuleTbl.getId())
                        .setCheckType(redisConfigCheckRuleTbl.getCheckType()).setParam(redisConfigCheckRuleTbl.getParam())
                        .setDescription(redisConfigCheckRuleTbl.getDescription());

                return  redisConfigCheckRuleCreateInfo;
            }
        }));
    }

    @Override
    public List<RedisConfigCheckRuleCreateInfo> getAllRedisConfigCheckRuleInfos() {
        return Lists.newArrayList(Lists.transform(getAllRedisConfigCheckRules(), new Function<RedisConfigCheckRuleTbl, RedisConfigCheckRuleCreateInfo>() {
            @Override
            public RedisConfigCheckRuleCreateInfo apply(RedisConfigCheckRuleTbl redisConfigCheckRuleTbl) {
                RedisConfigCheckRuleCreateInfo redisConfigCheckRuleCreateInfo =
                        new RedisConfigCheckRuleCreateInfo().setId(redisConfigCheckRuleTbl.getId())
                                .setCheckType(redisConfigCheckRuleTbl.getCheckType()).setParam(redisConfigCheckRuleTbl.getParam())
                                .setDescription(redisConfigCheckRuleTbl.getDescription());

                return  redisConfigCheckRuleCreateInfo;
            }
        }));
    }


    boolean redisConfigCheckRuleIsExist(RedisConfigCheckRuleCreateInfo redisConfigCheckRuleCreateInfo){
        RedisConfigCheckRuleTbl exist = getRedisConifgCheckRuleByParam(redisConfigCheckRuleCreateInfo.getParam());
        return exist != null;
    }
}
