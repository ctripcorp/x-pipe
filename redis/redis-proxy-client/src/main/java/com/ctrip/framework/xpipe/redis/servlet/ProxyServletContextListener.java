package com.ctrip.framework.xpipe.redis.servlet;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.ServletRegistration;
import javax.servlet.annotation.WebListener;

@WebListener
public class ProxyServletContextListener implements ServletContextListener {

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        ServletContext context = sce.getServletContext();

        try {
            Class.forName("javax.servlet.ServletRegistration");
            ServletRegistration.Dynamic asr = context.addServlet("ProxyServlet", ProxyServlet.class);
            if (asr != null) {
                asr.setLoadOnStartup(Integer.MAX_VALUE);
            }
        } catch (ClassNotFoundException e) {
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {

    }

}
