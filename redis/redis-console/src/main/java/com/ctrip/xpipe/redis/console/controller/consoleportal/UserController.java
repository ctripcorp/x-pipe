package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.api.sso.LogoutHandler;
import com.ctrip.xpipe.api.sso.UserInfo;
import com.ctrip.xpipe.api.sso.UserInfoHolder;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * @author lepdou 2016-11-08
 */
@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class UserController extends AbstractConsoleController{

  @Autowired
  private UserInfoHolder userInfoHolder;
  @Autowired
  private LogoutHandler logoutHandler;

  @RequestMapping("/user/current")
  public UserInfo getCurrentUserName() {
    return userInfoHolder.getUser();
  }

  @RequestMapping("/user/logout")
  public void logout(HttpServletRequest request, HttpServletResponse response){
    logoutHandler.logout(request, response);
  }

}
