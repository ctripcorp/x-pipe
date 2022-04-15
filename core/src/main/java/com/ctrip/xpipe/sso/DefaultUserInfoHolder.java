package com.ctrip.xpipe.sso;


import com.ctrip.xpipe.api.sso.UserInfo;
import com.ctrip.xpipe.api.sso.UserInfoHolder;

/**
 * @author lepdou 2016-11-08
 */
public class DefaultUserInfoHolder implements UserInfoHolder {

    private UserInfo defaultUser = new DefaultUserInfo();

    @Override
    public UserInfo getUser() {
        return defaultUser;
    }

    @Override
    public Object getContext() {
        return new Object();
    }

    @Override
    public void setContext(Object context) {

    }

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }

}
