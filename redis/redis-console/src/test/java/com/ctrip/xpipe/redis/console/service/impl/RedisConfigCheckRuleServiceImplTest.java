package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.controller.api.data.meta.RedisConfigCheckRuleCreateInfo;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class RedisConfigCheckRuleServiceImplTest extends AbstractServiceImplTest{

    @Autowired
    private RedisConfigCheckRuleServiceImpl redisConfigCheckRuleService;

    @Test
    public void testAddRedisConfigRuleSuccess() {
        RedisConfigCheckRuleCreateInfo redisConfigCheckRule = new RedisConfigCheckRuleCreateInfo();
        redisConfigCheckRule.setCheckType("config").setParam("{ 'configName': 'repl_backlog_size', 'expectedValue':256, whetherFix:true}");

        Assert.assertEquals(0, redisConfigCheckRuleService.getAllRedisConfigCheckRules().size());

        redisConfigCheckRuleService.addRedisConfigCHeckRule(redisConfigCheckRule);

        Assert.assertEquals(1, redisConfigCheckRuleService.getAllRedisConfigCheckRules().size());

    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddRedisConfigRuleFailBySameParams() {
        RedisConfigCheckRuleCreateInfo redisConfigCheckRule = new RedisConfigCheckRuleCreateInfo();
        redisConfigCheckRule.setCheckType("config").setParam("{ 'configName': 'repl_backlog_size', 'expectedValue':256, whetherFix:true}");

        Assert.assertEquals(0, redisConfigCheckRuleService.getAllRedisConfigCheckRules().size());

        redisConfigCheckRuleService.addRedisConfigCHeckRule(redisConfigCheckRule);

        Assert.assertEquals(1, redisConfigCheckRuleService.getAllRedisConfigCheckRules().size());

        try {
            redisConfigCheckRuleService.addRedisConfigCHeckRule(redisConfigCheckRule);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Redis config check rule : " + redisConfigCheckRule.getCheckType() + redisConfigCheckRule.getParam() + " already exists");
            throw e;
        }
    }

    @Test
    public void testUpdateRedisConfigRuleSuccess() {
        RedisConfigCheckRuleCreateInfo redisConfigCheckRule = new RedisConfigCheckRuleCreateInfo();
        String oldParam = "{ 'configName': 'repl_backlog_size', 'expectedValue':256, whetherFix:true}";
        String newParam = "{ 'configName': 'repl_backlog_size', 'expectedValue':256, whetherFix:false}";
        redisConfigCheckRule.setCheckType("config").setParam(oldParam);
        redisConfigCheckRuleService.addRedisConfigCHeckRule(redisConfigCheckRule);
        Assert.assertEquals(oldParam, redisConfigCheckRuleService.getRedisConifgCheckRuleById(1L).getParam());

        redisConfigCheckRule.setParam(newParam).setId(1L);
        redisConfigCheckRuleService.updateRedisConfigCHeckRule(redisConfigCheckRule);
        Assert.assertEquals(newParam, redisConfigCheckRuleService.getRedisConifgCheckRuleById(1L).getParam());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateRedisConfigRuleFailByNotFound() {
        RedisConfigCheckRuleCreateInfo redisConfigCheckRule = new RedisConfigCheckRuleCreateInfo();
        redisConfigCheckRule.setCheckType("config").setParam("{ 'configName': 'repl_backlog_size', 'expectedValue':256, whetherFix:true}").setId(1L);

        Assert.assertEquals(0, redisConfigCheckRuleService.getAllRedisConfigCheckRules().size());

        try {
            redisConfigCheckRuleService.updateRedisConfigCHeckRule(redisConfigCheckRule);
        } catch (Exception e) {
            Assert.assertEquals(String.format("Redis config check rule %s %s not found", redisConfigCheckRule.getCheckType(), redisConfigCheckRule.getParam()), e.getMessage());
            throw e;
        }
    }

    @Test
    public void testDeleteRedisConfigRuleByIdSuccess() {
        RedisConfigCheckRuleCreateInfo redisConfigCheckRule = new RedisConfigCheckRuleCreateInfo();
        redisConfigCheckRule.setCheckType("config").setParam("{ 'configName': 'repl_backlog_size', 'expectedValue':256, whetherFix:true}");

        Assert.assertEquals(0, redisConfigCheckRuleService.getAllRedisConfigCheckRules().size());

        redisConfigCheckRuleService.addRedisConfigCHeckRule(redisConfigCheckRule);

        Assert.assertEquals(1, redisConfigCheckRuleService.getAllRedisConfigCheckRules().size());

        redisConfigCheckRuleService.deleteRedisConfigCheckRuleById(1L);

        Assert.assertEquals(0, redisConfigCheckRuleService.getAllRedisConfigCheckRules().size());
    }

    @Test(expected = BadRequestException.class)
    public void testDeleteRedisConfigRuleByIdFailByNotExist() {
        Assert.assertEquals(0, redisConfigCheckRuleService.getAllRedisConfigCheckRules().size());
        try {
            redisConfigCheckRuleService.deleteRedisConfigCheckRuleById(1L);
        } catch (Exception e) {
            Assert.assertEquals(String.format("Redis config check rule %d not found", 1), e.getMessage());
            throw e;
        }
    }

    @Test
    public void testDeleteRedisConfigRuleByParamSuccess() {
        RedisConfigCheckRuleCreateInfo redisConfigCheckRule = new RedisConfigCheckRuleCreateInfo();
        String param = "{ 'configName': 'repl_backlog_size', 'expectedValue':256, whetherFix:true}";
        redisConfigCheckRule.setCheckType("config").setParam(param);

        Assert.assertEquals(0, redisConfigCheckRuleService.getAllRedisConfigCheckRules().size());

        redisConfigCheckRuleService.addRedisConfigCHeckRule(redisConfigCheckRule);

        Assert.assertEquals(1, redisConfigCheckRuleService.getAllRedisConfigCheckRules().size());

        redisConfigCheckRuleService.deleteRedisConfigCheckRuleByParam(param);

        Assert.assertEquals(0, redisConfigCheckRuleService.getAllRedisConfigCheckRules().size());
    }

    @Test(expected = BadRequestException.class)
    public void testDeleteRedisConfigRuleByParamFailByNotExist() {
        String param = "{ 'configName': 'repl_backlog_size', 'expectedValue':256, whetherFix:true}";

        Assert.assertEquals(0, redisConfigCheckRuleService.getAllRedisConfigCheckRules().size());
        try {
            redisConfigCheckRuleService.deleteRedisConfigCheckRuleByParam(param);
        } catch (Exception e) {
            Assert.assertEquals(String.format("Redis config check rule %s not found", param), e.getMessage());
            throw e;
        }
    }

    @Test
    public void testGetRedisConfigRuleSuccess(){
        RedisConfigCheckRuleCreateInfo redisConfigCheckRule1 = new RedisConfigCheckRuleCreateInfo();
        RedisConfigCheckRuleCreateInfo redisConfigCheckRule2 = new RedisConfigCheckRuleCreateInfo();
        String parm1 = "{ 'configName': 'repl_backlog_size', 'expectedValue':256, whetherFix:true}";
        String param2 = "{ 'configName': 'repl_backlog_size', 'expectedValue':256, whetherFix:false}";

        redisConfigCheckRule1.setCheckType("config").setParam(parm1);
        redisConfigCheckRule2.setCheckType("config").setParam(param2);

        redisConfigCheckRuleService.addRedisConfigCHeckRule(redisConfigCheckRule1);
        redisConfigCheckRuleService.addRedisConfigCHeckRule(redisConfigCheckRule2);

        Assert.assertEquals(2, redisConfigCheckRuleService.getAllRedisConfigCheckRules().size());
        Assert.assertEquals(1, redisConfigCheckRuleService.getRedisConifgCheckRuleByParam(parm1).getId());
        Assert.assertEquals(param2, redisConfigCheckRuleService.getRedisConifgCheckRuleById(2L).getParam());

    }

}
