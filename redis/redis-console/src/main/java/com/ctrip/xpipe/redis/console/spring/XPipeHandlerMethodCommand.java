package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.api.sso.UserInfoHolder;
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
    public final UserInfoHolder userInfoHolder;
    public final Object userInfoContext;
    public final Object[] args;

    public XPipeHandlerMethodCommand(Method method, Object bean,
                                     UserInfoHolder userInfoHolder,
                                     Object... args) {
        this.method = method;
        this.bean = bean;
        this.userInfoHolder = userInfoHolder;
        this.userInfoContext = userInfoHolder.getContext();
        this.args = args;
    }

    @Override
    public String getName() {
        return "XPipeHandlerMethodCommand";
    }

    @Override
    protected void doExecute() throws Throwable {
        try {
            userInfoHolder.setContext(userInfoContext);
            future().setSuccess(method.invoke(bean, args));
        } catch (Throwable t) {
            future().setFailure(t);
        }
    }

    @Override
    protected void doReset() {

    }
}
