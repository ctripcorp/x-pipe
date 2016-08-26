package com.ctrip.xpipe.redis.console.rest.consoleweb;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.List;

/**
 * @author zhangle 16/8/24
 */
public class XpipeController {

  @RequestMapping(value = "/ips/{ip}/ports")
  public List<Integer> findUsedPorts(@PathVariable String ip){
	  // TODO
    return null;
  }

}
