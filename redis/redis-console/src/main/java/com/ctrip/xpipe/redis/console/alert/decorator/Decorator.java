package com.ctrip.xpipe.redis.console.alert.decorator;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.utils.DateTimeUtils;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.io.StringWriter;

/**
 * @author chen.zhu
 * <p>
 * Oct 19, 2017
 */

public abstract class Decorator {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    protected VelocityEngine velocityEngine;

    @Autowired
    protected ConsoleConfig consoleConfig;

    private DateTimeUtils dateTimeUtils = new DateTimeUtils();

    private FoundationService foundationService = FoundationService.DEFAULT;

    private static final int MAX_LENGTH = 128;

    public String generateContent(AlertEntity alert) {
        VelocityContext context = generateCommonContext();
        alert.removeSpecialCharacters();
        context = fillInContext(alert, context);
        return getRenderedString(getTemplateName(), context);
    }


    protected VelocityContext generateCommonContext() {
        VelocityContext context = new VelocityContext();
        context.put("time", DateTimeUtils.currentTimeAsString());
        context.put("environment", consoleConfig.getXpipeRuntimeEnvironmentEnvironment());
        context.put("xpipeAdminEmails", consoleConfig.getXPipeAdminEmails());
        context.put("localIpAddr", foundationService.getLocalIp());
        context.put("dateTimeUtils", dateTimeUtils);
        context.put("xpipeurl", consoleConfig.getConsoleDomain());
        return context;
    }

    public String getRenderedString(String templateName, VelocityContext context) {
        StringWriter stringWriter = new StringWriter();
        String encoding = "UTF-8";
        try {
            velocityEngine.mergeTemplate(templateName, encoding, context, stringWriter);
            return stringWriter.toString();
        } catch (Exception e) {
            logger.error("[getRenderedString] Error with velocity:\n{}", e);
        } finally {
            try {
                stringWriter.close();
            } catch (IOException e) {
                logger.error("[getRenderedString] Closing string writer error:\n {}", e);
            }
        }
        return null;
    }

    protected abstract String getTemplateName();

    public String generateTitle(AlertEntity alert) {
        return shorten(doGenerateTitle(alert));
    }

    private String shorten(String title) {
        if(title.length() > MAX_LENGTH) {
            return title.substring(0, MAX_LENGTH) + "...";
        }
        return title;
    }

    protected abstract String doGenerateTitle(AlertEntity alert);

    protected abstract VelocityContext fillInContext(AlertEntity alert, VelocityContext context);
}
