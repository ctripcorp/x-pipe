package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.CheckFailException;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.RedisConfigCheckRuleCreateInfo;
import com.ctrip.xpipe.redis.console.service.impl.RedisConfigCheckRuleServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;

@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class RedisConfigCheckController extends AbstractConsoleController {
    @Autowired
    RedisConfigCheckRuleServiceImpl redisConfigCheckRuleService;

    @RequestMapping(value = "/redisConfigCheck", method = RequestMethod.POST)
    public RetMessage addRedisConfigCheckRule(@RequestBody RedisConfigCheckRuleCreateInfo createInfo) {
        try {
            createInfo.check();
            redisConfigCheckRuleService.addRedisConfigCHeckRule(createInfo);
            return  RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[addRedisConfigCheckRule][fail] {}", createInfo, th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    @RequestMapping(value = "/redisConfigCheck", method = RequestMethod.PUT)
    public RetMessage updateRedisConfigCheckRule(@RequestBody RedisConfigCheckRuleCreateInfo createInfo) {
        try {
            createInfo.check();
            redisConfigCheckRuleService.updateRedisConfigCHeckRule(createInfo);
            return  RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[updateRedisConfigCheckRule][fail] {}", createInfo, th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    @RequestMapping(value = "/redisConfigCheck/{ruleId}", method = RequestMethod.DELETE)
    public RetMessage deleteRedisConfigCheckRule(@PathVariable Long ruleId) {
        try {
            logger.info("[deleteRedisConfigCheckRule] {}", ruleId);
            redisConfigCheckRuleService.deleteRedisConfigCheckRuleById(ruleId);
            return  RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.error("[deleteRedisConfigCheckRule][fail] {}", ruleId, th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    @RequestMapping(value = "/redisConfigCheck/all", method = RequestMethod.GET)
    public List<RedisConfigCheckRuleCreateInfo> getAllRedisConfigCheckRuleInfos() {
        try {
            return redisConfigCheckRuleService.getAllRedisConfigCheckRuleInfos();
        } catch (Throwable th) {
            logger.error("[getAllRedisConfigCheckRuleInfos][fail] {}", th);
            return Collections.emptyList();
        }
    }

}
