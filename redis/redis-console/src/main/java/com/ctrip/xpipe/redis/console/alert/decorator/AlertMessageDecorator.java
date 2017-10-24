package com.ctrip.xpipe.redis.console.alert.decorator;

import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import org.apache.velocity.VelocityContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author chen.zhu
 * <p>
 * Oct 19, 2017
 */
@Component(AlertMessageDecorator.ID)
public class AlertMessageDecorator extends Decorator {

    public static final String ID = "alert.message.email.decorator";

    private static final String TEMPLATE_NAME = "RedisAlertTemplate.vm";

    @Override
    protected String getTemplateName() {
        return TEMPLATE_NAME;
    }

    @Override
    public String generateTitle(AlertEntity alert) {
        return String.format("[%s][XPipe 报警]%s",
                consoleConfig.getXpipeRuntimeEnvironmentEnvironment(),
                alert.getKey());
    }

    @Override
    protected VelocityContext fillInContext(AlertEntity alert, VelocityContext context) {
        context.put("" + alert.getAlertType(), alert.getAlertType());
        context.put("redisAlert", alert);
        context.put("title", generateTitle(alert));
        return context;
    }
}
