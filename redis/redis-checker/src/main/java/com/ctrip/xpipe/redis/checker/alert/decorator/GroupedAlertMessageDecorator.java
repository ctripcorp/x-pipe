package com.ctrip.xpipe.redis.checker.alert.decorator;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import org.apache.velocity.VelocityContext;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

/**
 * @author chen.zhu
 * <p>
 * Oct 19, 2017
 */
@Component(GroupedAlertMessageDecorator.ID)
public class GroupedAlertMessageDecorator extends Decorator {

    public static final String ID = "grouped.alert.message.email.decorator";

    private static final String TEMPLATE_NAME = "templates/ScheduledAlertTemplate.vm";

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

    public String generateTitle() {
        return String.format("[%s][XPipe 报警]",
                alertConfig.getXpipeRuntimeEnvironment());
    }

    @Override
    protected VelocityContext fillInContext(AlertEntity alert, VelocityContext context) {
        return context;
    }


    public String generateBody(Map<ALERT_TYPE, Set<AlertEntity>> alerts) {
        VelocityContext context = generateCommonContext();
        context.put("redisAlerts", alerts);
        context.put("title", generateTitle());
        return getRenderedString(getTemplateName(), context);
    }
}
