package com.ctrip.xpipe.redis.checker.alert.decorator;

import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import org.apache.velocity.VelocityContext;
import org.springframework.stereotype.Component;

/**
 * @author chen.zhu
 * <p>
 * Oct 19, 2017
 */
@Component(AlertMessageDecorator.ID)
public class AlertMessageDecorator extends Decorator {

    public static final String ID = "alert.message.email.decorator";

    private static final String TEMPLATE_NAME = "templates/RedisAlertTemplate.vm";

    @Override
    protected String getTemplateName() {
        return TEMPLATE_NAME;
    }

    @Override
    public String doGenerateTitle(AlertEntity alert) {
        return String.format("[%s][XPipe 报警]%s",
                alertConfig.getXpipeRuntimeEnvironment(),
                alert.getKey());
    }

    @Override
    protected VelocityContext fillInContext(AlertEntity alert, VelocityContext context) {
        context.put(alert.getAlertType().name(), alert.getAlertType());
        context.put("redisAlert", alert);
        context.put("title", generateTitle(alert));
        return context;
    }
}
