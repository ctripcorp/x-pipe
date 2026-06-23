package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.RedisCheckRuleCreateInfo;
import com.ctrip.xpipe.redis.console.model.RedisCheckRuleTbl;
import com.ctrip.xpipe.redis.console.resources.ConsolePortalService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisCheckRuleServiceWithoutDBTest {

    @Mock
    private ConsolePortalService consolePortalService;

    @Mock
    private ConsoleConfig config;

    @InjectMocks
    private RedisCheckRuleServiceWithoutDB redisCheckRuleService;

    @Before
    public void setUp() {
        when(config.getCacheRefreshInterval()).thenReturn(Integer.MAX_VALUE);
        when(consolePortalService.getAllRedisCheckRuleInfos()).thenReturn(portalRules());
        redisCheckRuleService.init();
    }

    @Test
    public void testGetAllRedisCheckRulesFromPortal() {
        List<RedisCheckRuleTbl> rules = redisCheckRuleService.getAllRedisCheckRules();

        Assert.assertEquals(2, rules.size());
        Assert.assertEquals(1L, rules.get(0).getId());
        Assert.assertEquals("config", rules.get(0).getCheckType());
        Assert.assertEquals("param-a", rules.get(0).getParam());
        Assert.assertEquals("desc-a", rules.get(0).getDescription());
        Assert.assertEquals("info", rules.get(1).getCheckType());
    }

    @Test
    public void testGetAllRedisCheckRulesWhenPortalReturnsNull() {
        when(consolePortalService.getAllRedisCheckRuleInfos()).thenReturn(null);
        redisCheckRuleService.init();

        Assert.assertTrue(redisCheckRuleService.getAllRedisCheckRules().isEmpty());
    }

    @Test
    public void testGetRedisCheckRuleById() {
        RedisCheckRuleTbl rule = redisCheckRuleService.getRedisCheckRuleById(2L);

        Assert.assertNotNull(rule);
        Assert.assertEquals("info", rule.getCheckType());
        Assert.assertEquals("param-b", rule.getParam());
    }

    @Test
    public void testGetRedisCheckRuleByIdNotFound() {
        Assert.assertNull(redisCheckRuleService.getRedisCheckRuleById(99L));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWriteOperationUnsupported() {
        redisCheckRuleService.addRedisCheckRule(new RedisCheckRuleCreateInfo()
                .setCheckType("config").setParam("param"));
    }

    private List<RedisCheckRuleCreateInfo> portalRules() {
        return Arrays.asList(
                new RedisCheckRuleCreateInfo().setId(1L).setCheckType("config")
                        .setParam("param-a").setDescription("desc-a"),
                new RedisCheckRuleCreateInfo().setId(2L).setCheckType("info")
                        .setParam("param-b").setDescription("desc-b")
        );
    }
}
