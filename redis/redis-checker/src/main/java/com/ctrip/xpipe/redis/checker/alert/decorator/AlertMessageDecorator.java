package com.ctrip.xpipe.redis.checker.alert.decorator;

import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import org.springframework.stereotype.Component;
// CHANGED: Import Thymeleaf's Context instead of Velocity's
import org.thymeleaf.context.Context;

/**
 * @author chen.zhu
 * <p>
 * Oct 19, 2017
 */
@Component(AlertMessageDecorator.ID)
public class AlertMessageDecorator extends Decorator {

    public static final String ID = "alert.message.email.decorator";

    // CHANGED: Template file extension is now .html
    private static final String TEMPLATE_NAME = "RedisAlertTemplate";

    @Override
    protected String getTemplateName() {
        return TEMPLATE_NAME;
    }

    @Override
    public String doGenerateTitle(AlertEntity alert) {
        // This method does not need any changes as it's pure Java logic.
        return String.format("[%s][XPipe 报警]%s",
                alertConfig.getXpipeRuntimeEnvironment(),
                alert.getKey());
    }

    /**
     * Fills the Thymeleaf context with alert-specific data.
     * Note the changed method signature: it now returns void and accepts a Thymeleaf Context.
     */
    @Override
    protected void fillInContext(AlertEntity alert, Context context) {
        // CHANGED: Use context.setVariable() instead of context.put()
        context.setVariable(alert.getAlertType().name(), alert.getAlertType());
        context.setVariable("redisAlert", alert);
        context.setVariable("title", generateTitle(alert));
        // CHANGED: No 'return context;' statement is needed as the method returns void.
    }
}
