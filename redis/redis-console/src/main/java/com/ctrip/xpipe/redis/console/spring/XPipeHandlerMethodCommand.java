package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.command.AbstractCommand;

import java.lang.reflect.Method;

/**
 * @author Slight
 * <p>
 * Mar 17, 2022 11:06 PM
 */
public class XPipeHandlerMethodCommand extends AbstractCommand<Object> {

    public final Method method;
    public final Object bean;
    public final Object[] args;

    public XPipeHandlerMethodCommand(Method method, Object bean, Object... args) {
        this.method = method;
        this.bean = bean;
        this.args = args;
    }

    @Override
    public String getName() {
        return "XPipeHandlerMethodCommand";
    }

    @Override
    protected void doExecute() throws Throwable {
        try {
            future().setSuccess(method.invoke(bean, args));
        } catch (Throwable t) {
            future().setFailure(t);
        }
    }

    @Override
    protected void doReset() {

    }
}
