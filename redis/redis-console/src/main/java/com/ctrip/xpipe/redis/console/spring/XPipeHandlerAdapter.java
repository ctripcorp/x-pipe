package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import org.apache.tomcat.util.threads.TaskQueue;
import org.apache.tomcat.util.threads.TaskThreadFactory;
import org.apache.tomcat.util.threads.ThreadPoolExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.Ordered;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerAdapter;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;
import org.springframework.web.servlet.mvc.method.annotation.ServletInvocableHandlerMethod;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * @author Slight
 * <p>
 * Mar 15, 2022 1:37 AM
 */
public class XPipeHandlerAdapter extends RequestMappingHandlerAdapter implements HandlerAdapter, Ordered {

    public static int DEFAULT_CORE_POOl_SIZE = 600;
    public static int DEFAULT_KEEP_ALIVE_SECONDS = 60;

    public Executor executor = new ThreadPoolExecutor(DEFAULT_CORE_POOl_SIZE, DEFAULT_CORE_POOl_SIZE,
            DEFAULT_KEEP_ALIVE_SECONDS, TimeUnit.SECONDS, new TaskQueue(),
            new TaskThreadFactory("xpipe-handler-adapter-exec-", true, Thread.NORM_PRIORITY));

    @Autowired
    private ConsoleConfig config;

    @Override
    protected ServletInvocableHandlerMethod createInvocableHandlerMethod(HandlerMethod handlerMethod) {
        return new XPipeServletInvocableHandlerMethod(handlerMethod, executor, config.getServletMethodTimeoutMilli());
    }
}
