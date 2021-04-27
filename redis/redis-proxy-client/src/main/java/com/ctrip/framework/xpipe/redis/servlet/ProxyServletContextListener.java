package com.ctrip.framework.xpipe.redis.servlet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.ServletRegistration;
import javax.servlet.annotation.WebListener;

/**
 * @Author limingdong
 * @create 2021/4/26
 */
@WebListener
public class ProxyServletContextListener implements ServletContextListener {

    private static Logger logger = LoggerFactory.getLogger(ProxyServletContextListener.class);

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        logger.info("Begin Init Proxy");
        ServletContext context = sce.getServletContext();

        try {
            Class.forName("javax.servlet.ServletRegistration");
            ServletRegistration.Dynamic asr = context.addServlet("ProxyServlet", ProxyServlet.class);
            if (asr != null) {
                asr.setLoadOnStartup(Integer.MAX_VALUE);
            } else {
                logger.warn("Servlet ProxyServlet already exists");
            }
        } catch (ClassNotFoundException e) {
            logger.warn("servlet version below 3.0", e);
        }

        logger.info("End Init Proxy");

    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {

    }

}
