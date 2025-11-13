package com.ctrip.xpipe.redis.checker.alert.decorator;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.alert.AlertConfig;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.utils.DateTimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

/**
 * @author chen.zhu
 * <p>
 * Oct 19, 2017
 */
public abstract class Decorator {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    // CHANGED: Injected Thymeleaf's TemplateEngine instead of VelocityEngine
    @Autowired
    protected TemplateEngine templateEngine;

    @Autowired
    protected AlertConfig alertConfig;

    // This utility object will be passed to the Thymeleaf context
    private final DateTimeUtils dateTimeUtils = new DateTimeUtils();

    private final FoundationService foundationService = FoundationService.DEFAULT;

    private static final int MAX_LENGTH = 128;

    /**
     * Main method to generate the email body using Thymeleaf.
     */
    public String generateContent(AlertEntity alert) {
        // 1. Create Thymeleaf context and add common variables
        Context context = generateCommonContext();

        // Sanitize alert entity (this logic remains the same)
        alert.removeSpecialCharacters();

        // 2. Let subclasses add their specific variables
        fillInContext(alert, context);

        // 3. Render the template
        return getRenderedString(getTemplateName(), context);
    }

    /**
     * Generates a Thymeleaf context with common variables.
     * @return A Thymeleaf Context object.
     */
    protected Context generateCommonContext() {
        // CHANGED: Use org.thymeleaf.context.Context
        Context context = new Context();
        // CHANGED: Use context.setVariable() instead of context.put()
        context.setVariable("time", DateTimeUtils.currentTimeAsString());
        context.setVariable("environment", alertConfig.getXpipeRuntimeEnvironment());
        context.setVariable("xpipeAdminEmails", alertConfig.getXPipeAdminEmails());
        context.setVariable("localIpAddr", foundationService.getLocalIp());
        context.setVariable("dateTimeUtils", dateTimeUtils); // Pass the utility object
        context.setVariable("xpipeurl", alertConfig.getConsoleDomain());
        return context;
    }

    /**
     * Renders a Thymeleaf template into a String.
     * @param templateName The name of the template (e.g., "my-template.html")
     * @param context The Thymeleaf context with all the data.
     * @return The rendered HTML as a String.
     */
    public String getRenderedString(String templateName, Context context) {
        // CHANGED: The rendering logic is much simpler with Thymeleaf
        try {
            // templateEngine.process directly returns the rendered string. No StringWriter needed.
            return templateEngine.process(templateName, context);
        } catch (Exception e) {
            logger.error("[getRenderedString] Error with Thymeleaf rendering:\n", e);
        }
        return null; // Return null on failure
    }

    /**
     * Subclasses must implement this to provide the template file name.
     * IMPORTANT: The name should now end with .html (e.g., "alert-email.html").
     * @return The template name.
     */
    protected abstract String getTemplateName();

    public String generateTitle(AlertEntity alert) {
        return shorten(doGenerateTitle(alert));
    }

    private String shorten(String title) {
        if (title.length() > MAX_LENGTH) {
            return title.substring(0, MAX_LENGTH) + "...";
        }
        return title;
    }

    protected abstract String doGenerateTitle(AlertEntity alert);

    /**
     * CHANGED: The signature now uses Thymeleaf's Context and returns void.
     * Subclasses must implement this method to add alert-specific data to the context.
     * @param alert The alert entity.
     * @param context The Thymeleaf context to be filled.
     */
    protected abstract void fillInContext(AlertEntity alert, Context context);
}
