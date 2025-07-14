package com.ctrip.framework.xpipe.redis.servlet;

import com.ctrip.framework.xpipe.redis.instrument.ProxyAgentTool;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;


public class ProxyServlet extends HttpServlet {

    @Override
    public void init() throws ServletException {
        try {
            ProxyAgentTool.startUp();
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

}

