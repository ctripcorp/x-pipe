package com.ctrip.xpipe.redis.console.util;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.StringWriter;

/**
 * @author chen.zhu
 * <p>
 * Oct 12, 2017
 */

@Component
public class VelocityUtil {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private VelocityEngine velocityEngine;

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
}
