package com.ctrip.xpipe.redis.console.util;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.RedisAlert;
import org.apache.velocity.VelocityContext;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 12, 2017
 */
public class VelocityUtilTest extends AbstractConsoleIntegrationTest {

    @Autowired
    VelocityUtil velocityUtil;

    @Test
    public void getRenderedString() throws Exception {
        String templateName = "VelocityTestTemplate.vm";
        VelocityContext context = new VelocityContext();
        String test = "Hello World!";
        context.put("test", test);

        String text = velocityUtil.getRenderedString(templateName, context);
        Assert.assertEquals(test, text);
    }

    @Test
    public void getRenderedString2() throws Exception {
        String templateName = "ScheduledRedisAlertTemplate.vm";
        VelocityContext context = new VelocityContext();
        Map<ALERT_TYPE, Set<RedisAlert>> map = new HashMap<>();
        map.put(ALERT_TYPE.CLIENT_INCONSIS, generateRedisAlertSet(5, ALERT_TYPE.CLIENT_INCONSIS));
        map.put(ALERT_TYPE.MIGRATION_MANY_UNFINISHED, generateRedisAlertSet(5, ALERT_TYPE.MIGRATION_MANY_UNFINISHED));
        map.put(ALERT_TYPE.REDIS_REPL_DISKLESS_SYNC_ERROR, generateRedisAlertSet(5, ALERT_TYPE.REDIS_REPL_DISKLESS_SYNC_ERROR));

        context.put("redisAlerts", map);
        context.put("XREDIS_VERSION_NOT_VALID", ALERT_TYPE.XREDIS_VERSION_NOT_VALID);
        context.put("CLIENT_INCONSIS", ALERT_TYPE.CLIENT_INCONSIS);
        context.put("MIGRATION_MANY_UNFINISHED", ALERT_TYPE.MIGRATION_MANY_UNFINISHED);
        context.put("REDIS_REPL_DISKLESS_SYNC_ERROR", ALERT_TYPE.REDIS_REPL_DISKLESS_SYNC_ERROR);
        String text = velocityUtil.getRenderedString(templateName, context);
        logger.info("Text is as: \n{}", text);
    }

    private Set<RedisAlert> generateRedisAlertSet(int count, ALERT_TYPE alertType) {
        int index = 0;
        Set<RedisAlert> result = new HashSet<>();
        while(index++ < count) {
            RedisAlert redisAlert = new RedisAlert(new HostPort("192.168.1.10", index),
                    "cluster"+index, "shard"+index, "", alertType);
            result.add(redisAlert);
        }
        return result;
    }
}