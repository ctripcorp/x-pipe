package com.ctrip.xpipe.redis.console.alert.decorator;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import org.apache.velocity.VelocityContext;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

/**
 * @author chen.zhu
 * <p>
 * Oct 19, 2017
 */
@Component(ScheduledAlertMessageDecorator.ID)
public class ScheduledAlertMessageDecorator extends Decorator {

    public static final String ID = "scheduled.alert.message.email.decorator";

    private static final String TEMPLATE_NAME = "ScheduledAlertTemplate.vm";

    @Override
    protected String getTemplateName() {
        return TEMPLATE_NAME;
    }

    @Override
    public String doGenerateTitle(AlertEntity alert) {
        return String.format("[%s][XPipe 报警]%s",
                consoleConfig.getXpipeRuntimeEnvironmentEnvironment(),
                alert.getKey());
    }

    public String generateTitle() {
        return String.format("[%s][XPipe 报警]",
                consoleConfig.getXpipeRuntimeEnvironmentEnvironment());
    }

    @Override
    protected VelocityContext fillInContext(AlertEntity alert, VelocityContext context) {
        return context;
    }


    public String generateBody(Map<ALERT_TYPE, Set<AlertEntity>> redisAlerts) {
        VelocityContext context = generateCommonContext();
        for(ALERT_TYPE type : redisAlerts.keySet()) {
            context.put(type + "", type);
        }
        context.put("redisAlerts", redisAlerts);
        context.put("title", generateTitle());
        return getRenderedString(TEMPLATE_NAME, context);
    }
}
