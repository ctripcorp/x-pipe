package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.sso.UserInfoHolder;
import com.ctrip.xpipe.redis.console.controller.Health;
import com.ctrip.xpipe.redis.console.controller.api.migrate.MigrationApi;
import com.ctrip.xpipe.redis.console.controller.api.migrate.MigrationApi4Beacon;
import com.ctrip.xpipe.redis.console.controller.api.migrate.MigrationInfoApi;
import com.ctrip.xpipe.redis.console.controller.consoleportal.MigrationController;
import org.springframework.util.ReflectionUtils;
import org.springframework.web.context.request.async.DeferredResult;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.mvc.method.annotation.ServletInvocableHandlerMethod;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.Executor;

/**
 * @author Slight
 * <p>
 * Mar 16, 2022 4:05 PM
 */
public class XPipeServletInvocableHandlerMethod extends ServletInvocableHandlerMethod {

    public final Executor executor;
    public final long timeout;

    public static String EVENT_TYPE_SERVLET_HANDLER = "Servlet.Handler";

    public XPipeServletInvocableHandlerMethod(HandlerMethod handlerMethod, Executor executor, long timeout) {
        super(handlerMethod);
        this.executor = executor;
        this.timeout = timeout;
    }

    @Override
    protected Object doInvoke(Object... args) throws Exception {
        ReflectionUtils.makeAccessible(getBridgedMethod());
        try {
            /* <<-- ONLY THIS PART IS MODIFIED -- */
            if (getBean() instanceof MigrationApi
                    || getBean() instanceof MigrationApi4Beacon
                    || getBean() instanceof Health
                    || getBean() instanceof MigrationInfoApi
                    || getBean() instanceof MigrationController) {

                EventMonitor.DEFAULT.logEvent(EVENT_TYPE_SERVLET_HANDLER, "Sync");
                return getBridgedMethod().invoke(getBean(), args);
            } else {

                EventMonitor.DEFAULT.logEvent(EVENT_TYPE_SERVLET_HANDLER, "Async");
                DeferredResult<Object> result = new DeferredResult<>(timeout);
                Command<Object> command = new XPipeHandlerMethodCommand(getBridgedMethod(), getBean(), UserInfoHolder.DEFAULT, args);
                command.execute(executor).addListener(commandFuture -> {
                    if (commandFuture.isSuccess()) {
                        result.setResult(commandFuture.get());
                    } else {
                        result.setErrorResult(commandFuture.cause());
                    }
                });
                return result;
            }
            /* -- ONLY THIS PART IS MODIFIED -->> */

            /* ORIGIN CODE:  return getBridgedMethod().invoke(getBean(), args); */
        }
        catch (IllegalArgumentException ex) {
            assertTargetBean(getBridgedMethod(), getBean(), args);
            String text = (ex.getMessage() != null ? ex.getMessage() : "Illegal argument");
            throw new IllegalStateException(formatInvokeError(text, args), ex);
        }
        catch (InvocationTargetException ex) {
            // Unwrap for HandlerExceptionResolvers ...
            Throwable targetException = ex.getTargetException();
            if (targetException instanceof RuntimeException) {
                throw (RuntimeException) targetException;
            }
            else if (targetException instanceof Error) {
                throw (Error) targetException;
            }
            else if (targetException instanceof Exception) {
                throw (Exception) targetException;
            }
            else {
                throw new IllegalStateException(formatInvokeError("Invocation failure", args), targetException);
            }
        }
    }
}
