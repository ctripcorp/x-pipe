package com.ctrip.xpipe.service.sso;


import com.ctrip.xpipe.api.sso.UserInfo;
import com.ctrip.xpipe.api.sso.UserInfoHolder;

import java.lang.reflect.Method;

/**
 * @author lepdou 2016-11-08
 */
public class CtripUserInfoHolder implements UserInfoHolder {

    private Object assertionHolder;

    private Method getAssertion;


    public CtripUserInfoHolder() {
        Class clazz = null;
        try {
            clazz = Class.forName("org.jasig.cas.client.util.AssertionHolder");
            assertionHolder = clazz.newInstance();
            getAssertion = assertionHolder.getClass().getMethod("getAssertion");
        } catch (Exception e) {
            throw new RuntimeException("init AssertionHolder fail", e);
        }
    }

    @Override
    public UserInfo getUser() {
        try {

            Object assertion = getAssertion.invoke(assertionHolder);
            Method getPrincipal = assertion.getClass().getMethod("getPrincipal");
            Object principal = getPrincipal.invoke(assertion);
            Method getName = principal.getClass().getMethod("getName");
            String userId = (String) getName.invoke(principal);

            UserInfo userInfo = new CtripUserInfo();
            userInfo.setUserId(userId);

            return userInfo;

        } catch (Exception e) {
            throw new RuntimeException("get user info from assertion holder error", e);
        }
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }
}
