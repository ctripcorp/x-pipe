package com.ctrip.xpipe.api.sso;


import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.utils.ServicesUtil;

/**
 * @author lepdou 2016-11-08
 */
public interface UserInfoHolder extends Ordered {

  UserInfoHolder DEFAULT = ServicesUtil.getUserInfoHolder();

  UserInfo getUser();

  Object getContext();

  void setContext(Object context);

}
