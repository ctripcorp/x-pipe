package com.ctrip.xpipe.redis.console.alert.decorator;

import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import org.apache.velocity.VelocityContext;
import org.springframework.stereotype.Component;

/**
 * @author chen.zhu
 * <p>
 * Oct 19, 2017
 */
@Component(RecoverMessageDecorator.ID)
public class RecoverMessageDecorator extends Decorator {

    public static final String ID = "recover.message.email.decorator";

    private static final String TEMPLATE_NAME = "RecoverTemplate.vm";

    @Override
    protected String getTemplateName() {
        return TEMPLATE_NAME;
    }

    @Override
    public String doGenerateTitle(AlertEntity alert) {
        return String.format("[%s][XPipe 恢复]%s",
                consoleConfig.getXpipeRuntimeEnvironmentEnvironment(),
                alert.getKey());
    }

    @Override
    public String getRenderedString(String templateName, VelocityContext context) {
        return super.getRenderedString(templateName, context);
    }

    @Override
    protected VelocityContext fillInContext(AlertEntity alert, VelocityContext context) {
        context.put("redisAlert", alert);
        context.put("title", generateTitle(alert));
        return context;
    }
}
