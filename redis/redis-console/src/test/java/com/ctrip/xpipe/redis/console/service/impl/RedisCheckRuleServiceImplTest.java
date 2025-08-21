package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.controller.api.data.meta.DcClusterCreateInfo;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.RedisCheckRuleCreateInfo;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public class RedisCheckRuleServiceImplTest extends AbstractServiceImplTest{

    @Autowired
    private RedisCheckRuleServiceImpl redisCheckRuleService;

    @Autowired
    private DcClusterServiceImpl dcClusterService;

    @Test
    public void testAddRedisCheckRuleSuccess() {
        RedisCheckRuleCreateInfo redisCheckRule = new RedisCheckRuleCreateInfo();
        redisCheckRule.setCheckType("config").setParam("{ \"configName\": \"repl_backlog_size\", \"expectedValue\":256\"}");

        Assert.assertEquals(0, redisCheckRuleService.getAllRedisCheckRules().size());

        redisCheckRuleService.addRedisCheckRule(redisCheckRule);

        Assert.assertEquals(1, redisCheckRuleService.getAllRedisCheckRules().size());

    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddRedisRuleFailBySameParams() {
        RedisCheckRuleCreateInfo redisCheckRule = new RedisCheckRuleCreateInfo();
        redisCheckRule.setCheckType("config").setParam("{ \"configName\": \"repl_backlog_size\", \"expectedValue\":256\"}");

        Assert.assertEquals(0, redisCheckRuleService.getAllRedisCheckRules().size());

        redisCheckRuleService.addRedisCheckRule(redisCheckRule);

        Assert.assertEquals(1, redisCheckRuleService.getAllRedisCheckRules().size());

        try {
            redisCheckRuleService.addRedisCheckRule(redisCheckRule);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Redis config check rule : " + redisCheckRule.getCheckType() + redisCheckRule.getParam() + " already exists");
            throw e;
        }
    }

    @Test
    public void testUpdateRedisRuleSuccess() {
        RedisCheckRuleCreateInfo redisCheckRule = new RedisCheckRuleCreateInfo();
        String oldParam = "{ \"configName\": \"repl_backlog_size\", \"expectedValue\":256\"}";
        String newParam = "{ \"configName\": \"repl_backlog_size\", \"expectedValue\":2566\"}";
        redisCheckRule.setCheckType("config").setParam(oldParam);
        redisCheckRuleService.addRedisCheckRule(redisCheckRule);
        Assert.assertEquals(oldParam, redisCheckRuleService.getRedisCheckRuleById(1L).getParam());

        redisCheckRule.setParam(newParam).setId(1L);
        redisCheckRuleService.updateRedisCheckRule(redisCheckRule);
        Assert.assertEquals(newParam, redisCheckRuleService.getRedisCheckRuleById(1L).getParam());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateRedisRuleFailByNotFound() {
        RedisCheckRuleCreateInfo redisCheckRule = new RedisCheckRuleCreateInfo();
        redisCheckRule.setCheckType("config").setParam("{ \"configName\": \"repl_backlog_size\", \"expectedValue\":256\"}").setId(1L);

        Assert.assertEquals(0, redisCheckRuleService.getAllRedisCheckRules().size());

        try {
            redisCheckRuleService.updateRedisCheckRule(redisCheckRule);
        } catch (Exception e) {
            Assert.assertEquals(String.format("Redis config check rule %s %s not found", redisCheckRule.getCheckType(), redisCheckRule.getParam()), e.getMessage());
            throw e;
        }
    }

    @Test
    public void testDeleteRedisRuleByIdSuccess() {
        RedisCheckRuleCreateInfo redisCheckRule = new RedisCheckRuleCreateInfo();
        redisCheckRule.setCheckType("config").setParam("{ \"configName\": \"repl_backlog_size\", \"expectedValue\":256\"}");

        Assert.assertEquals(0, redisCheckRuleService.getAllRedisCheckRules().size());

        redisCheckRuleService.addRedisCheckRule(redisCheckRule);

        Assert.assertEquals(1, redisCheckRuleService.getAllRedisCheckRules().size());

        redisCheckRuleService.deleteRedisCheckRuleById(1L);

        Assert.assertEquals(0, redisCheckRuleService.getAllRedisCheckRules().size());
    }

    @Test
    public void testDeleteRedisRuleByIdFailByNotExist() {
        Assert.assertEquals(0, redisCheckRuleService.getAllRedisCheckRules().size());
        try {
            redisCheckRuleService.deleteRedisCheckRuleById(4L);
        } catch (Exception e) {
            Assert.assertEquals(String.format("Redis config check rule %d not found", 4), e.getMessage());
        }
    }

    @Test
    public void testDeleteRedisRuleByParamSuccess() {
        RedisCheckRuleCreateInfo redisCheckRule = new RedisCheckRuleCreateInfo();
        String param = "{ \"configName\": \"repl_backlog_size\", \"expectedValue\":256\"}";
        redisCheckRule.setCheckType("config").setParam(param);

        Assert.assertEquals(0, redisCheckRuleService.getAllRedisCheckRules().size());
        redisCheckRuleService.addRedisCheckRule(redisCheckRule);
        Assert.assertEquals(1, redisCheckRuleService.getAllRedisCheckRules().size());

        List<DcClusterCreateInfo> dcClusterCreateInfos = dcClusterService.findClusterRelated("cluster1");
        for (DcClusterCreateInfo dcClusterCreateInfo : dcClusterCreateInfos) {
            Assert.assertEquals(null, dcClusterCreateInfo.getRedisCheckRule());
            dcClusterCreateInfo.setRedisCheckRule("1");
            dcClusterService.updateDcCluster(dcClusterCreateInfo);
        }
        dcClusterService.findClusterRelated("cluster1").forEach(dcClusterCreateInfo -> Assert.assertEquals("1", dcClusterCreateInfo.getRedisCheckRule()));

        redisCheckRuleService.deleteRedisCheckRuleByParam(param);
        Assert.assertEquals(0, redisCheckRuleService.getAllRedisCheckRules().size());
    }

    @Test
    public void testDeleteRedisRuleByParamFailByNotExist() {
        String param = "{ \"configName\": \"repl_backlog_size\", \"expectedValue\":256\"}";

        Assert.assertEquals(0, redisCheckRuleService.getAllRedisCheckRules().size());
        try {
            redisCheckRuleService.deleteRedisCheckRuleByParam(param);
        } catch (Exception e) {
            Assert.assertEquals(String.format("Redis config check rule %s not found", param), e.getMessage());
        }
    }

    @Test
    public void testGetRedisRuleSuccess(){
        RedisCheckRuleCreateInfo redisCheckRule1 = new RedisCheckRuleCreateInfo();
        RedisCheckRuleCreateInfo redisCheckRule2 = new RedisCheckRuleCreateInfo();
        String parm1 = "{ \"configName\": \"repl_backlog_size\", \"expectedValue\":256\"}";
        String param2 = "{ { \"configName\": \"repl_backlog_size\", \"expectedValue\":2566\"}";

        redisCheckRule1.setCheckType("config").setParam(parm1);
        redisCheckRule2.setCheckType("config").setParam(param2);

        redisCheckRuleService.addRedisCheckRule(redisCheckRule1);
        redisCheckRuleService.addRedisCheckRule(redisCheckRule2);

        Assert.assertEquals(2, redisCheckRuleService.getAllRedisCheckRules().size());
        Assert.assertEquals(1, redisCheckRuleService.getRedisCheckRuleByParam(parm1).getId());
        Assert.assertEquals(param2, redisCheckRuleService.getRedisCheckRuleById(2L).getParam());
    }

}
