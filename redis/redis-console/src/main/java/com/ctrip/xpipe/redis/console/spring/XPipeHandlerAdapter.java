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

    public static int TASK_QUEUE_SIZE = 200;
    public static int CORE_POOl_SIZE = 200;
    public static int MAX_POOl_SIZE = 600;
    public static int KEEP_ALIVE_SECONDS = 60;

    public Executor executor = executor();

    @Autowired
    private ConsoleConfig config;

    private ThreadPoolExecutor executor() {
        TaskQueue taskqueue = new TaskQueue(TASK_QUEUE_SIZE);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(CORE_POOl_SIZE, MAX_POOl_SIZE,
                KEEP_ALIVE_SECONDS, TimeUnit.SECONDS, taskqueue,
                new TaskThreadFactory("xpipe-handler-adapter-exec-", true, Thread.NORM_PRIORITY));
        taskqueue.setParent(executor);
        return executor;
    }

    @Override
    protected ServletInvocableHandlerMethod createInvocableHandlerMethod(HandlerMethod handlerMethod) {
        return new XPipeServletInvocableHandlerMethod(handlerMethod, executor, config.getServletMethodTimeoutMilli());
    }
}
