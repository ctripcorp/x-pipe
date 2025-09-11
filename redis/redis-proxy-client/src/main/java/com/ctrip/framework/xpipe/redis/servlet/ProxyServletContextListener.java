package com.ctrip.framework.xpipe.redis.servlet;


import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import jakarta.servlet.ServletRegistration;
import jakarta.servlet.annotation.WebListener;

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
