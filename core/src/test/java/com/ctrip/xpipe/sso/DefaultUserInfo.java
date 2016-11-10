package com.ctrip.xpipe.sso;

import com.ctrip.xpipe.api.sso.UserInfo;

/**
 * @author lepdou 2016-11-08
 */
public class DefaultUserInfo implements UserInfo{

    @Override
    public String getUserId() {
        return "xpipe";
    }

    @Override
    public void setUserId(String userId) {

    }

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }
}
