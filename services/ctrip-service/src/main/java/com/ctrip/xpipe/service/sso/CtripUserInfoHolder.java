package com.ctrip.xpipe.service.sso;


import com.ctrip.infosec.sso.client.principal.Assertion;
import com.ctrip.infosec.sso.client.principal.AttributePrincipal;
import com.ctrip.infosec.sso.client.util.AssertionHolder;
import com.ctrip.xpipe.api.sso.UserInfo;
import com.ctrip.xpipe.api.sso.UserInfoHolder;

/**
 * @author lepdou 2016-11-08
 */
public class CtripUserInfoHolder implements UserInfoHolder {

    public CtripUserInfoHolder() {
    }

    @Override
    public UserInfo getUser() {
        try {

            Assertion assertion = AssertionHolder.getAssertion();

            if(assertion != null){
                AttributePrincipal principal = assertion.getPrincipal();
                String userId = principal.getName();

                UserInfo userInfo = new CtripUserInfo();
                userInfo.setUserId(userId);
                return userInfo;
            }
        } catch (Exception e) {
            throw new RuntimeException("get user info from assertion holder error", e);
        }

        return CtripUserInfo.noBody();
    }

    @Override
    public Object getContext() {
        return AssertionHolder.getAssertion();
    }

    @Override
    public void setContext(Object context) {
        AssertionHolder.setAssertion((Assertion) context);
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }
}
