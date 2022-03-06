package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.RedisCheckRuleCreateInfo;
import com.ctrip.xpipe.redis.console.service.impl.RedisCheckRuleServiceImpl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;

@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class RedisCheckController extends AbstractConsoleController {
    @Autowired
    RedisCheckRuleServiceImpl redisCheckRuleService;

    @RequestMapping(value = "/redisCheck", method = RequestMethod.POST)
    public RetMessage addRedisCheckRule(@RequestBody RedisCheckRuleCreateInfo createInfo) {
        try {
            createInfo.check();
            redisCheckRuleService.addRedisCheckRule(createInfo);
            return  RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[addRedisCheckRule][fail] {}", createInfo, th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    @RequestMapping(value = "/redisCheck", method = RequestMethod.PUT)
    public RetMessage updateRedisCheckRule(@RequestBody RedisCheckRuleCreateInfo createInfo) {
        try {
            createInfo.check();
            redisCheckRuleService.updateRedisCheckRule(createInfo);
            return  RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[updateRedisCheckRule][fail] {}", createInfo, th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    @RequestMapping(value = "/redisCheck/{ruleId}", method = RequestMethod.DELETE)
    public RetMessage deleteRedisCheckRule(@PathVariable Long ruleId) {
        try {
            logger.info("[deleteRedisCheckRule] {}", ruleId);
            redisCheckRuleService.deleteRedisCheckRuleById(ruleId);
            return RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[deleteRedisCheckRule][fail] {}", ruleId, th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    @RequestMapping(value = "/redisCheck/all", method = RequestMethod.GET)
    public List<RedisCheckRuleCreateInfo> getAllRedisCheckRuleInfos() {
        try {
            return redisCheckRuleService.getAllRedisCheckRuleInfos();
        } catch (Throwable th) {
            logger.error("[getAllRedisCheckRuleInfos][fail] {}", th);
            return Collections.emptyList();
        }
    }

}
