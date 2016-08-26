package com.ctrip.xpipe.redis.console.enums;

/**
 * @author zhangle 16/8/24
 */
public enum RedisRole {

  KEEPER("keeper"), REDIS("redis");
  private String value;

  RedisRole(String value) {
    this.value = value;
  }

  public String getValue(){
    return value;
  }
}
