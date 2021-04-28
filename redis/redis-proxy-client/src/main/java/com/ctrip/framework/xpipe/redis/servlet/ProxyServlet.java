package com.ctrip.framework.xpipe.redis.servlet;

import com.alibaba.arthas.deps.org.slf4j.Logger;
import com.alibaba.arthas.deps.org.slf4j.LoggerFactory;
import com.ctrip.framework.xpipe.redis.instrument.ProxyAgentTool;

import javax.servlet.http.HttpServlet;

/**
 * @Author limingdong
 * @create 2021/4/26
 */
public class ProxyServlet extends HttpServlet {

    private static Logger logger = LoggerFactory.getLogger(ProxyServlet.class);

    @Override
    public void init() {
        try {
            ProxyAgentTool.startUp();
        } catch (Throwable e) {
            logger.error("Proxy init failed", e);
        }
    }

}

