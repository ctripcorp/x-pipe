package com.ctrip.xpipe.redis.checker.alert.decorator;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import org.springframework.stereotype.Component;
// CHANGED: Import Thymeleaf's Context
import org.thymeleaf.context.Context;

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

    // CHANGED: Template file extension is now .html
    private static final String TEMPLATE_NAME = "ScheduledAlertTemplate";

    @Override
    protected String getTemplateName() {
        return TEMPLATE_NAME;
    }

    @Override
    public String doGenerateTitle(AlertEntity alert) {
        // No changes needed, pure Java logic
        return String.format("[%s][XPipe 报警]%s",
                alertConfig.getXpipeRuntimeEnvironment(),
                alert.getKey());
    }

    public String generateTitle() {
        // No changes needed, pure Java logic
        return String.format("[%s][XPipe 报警]",
                alertConfig.getXpipeRuntimeEnvironment());
    }

    /**
     * CHANGED: This method's signature is updated to comply with the abstract parent class.
     * In this specific decorator, the main logic is in generateBody(), so this method
     * is intentionally left empty (it's a "no-op"). It's required for polymorphism
     * but not used when calling generateBody() directly.
     */
    @Override
    protected void fillInContext(AlertEntity alert, Context context) {
        // This method is not used by the main logic path (generateBody), so it's empty.
    }

    /**
     * This is the main entry point for this decorator.
     * It has been updated to use Thymeleaf's Context.
     * @param alerts A map of alerts to be rendered.
     * @return The rendered HTML body as a String.
     */
    public String generateBody(Map<ALERT_TYPE, Set<AlertEntity>> alerts) {
        // CHANGED: generateCommonContext() now returns a Thymeleaf Context
        Context context = generateCommonContext();

        // CHANGED: Use context.setVariable() to add data to the context
        context.setVariable("redisAlerts", alerts);
        context.setVariable("title", generateTitle());

        // The getRenderedString method in the parent class already handles Thymeleaf rendering
        return getRenderedString(getTemplateName(), context);
    }
}
