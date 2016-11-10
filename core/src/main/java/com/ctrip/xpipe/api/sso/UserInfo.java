package com.ctrip.xpipe.api.sso;

import com.ctrip.xpipe.api.lifecycle.Ordered;

/**
 * @author lepdou 2016-11-08
 */
public interface UserInfo extends Ordered{

    String getUserId();

    void setUserId(String userId);

}
