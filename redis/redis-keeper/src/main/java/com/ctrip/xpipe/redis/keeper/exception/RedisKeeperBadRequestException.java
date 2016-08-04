package com.ctrip.xpipe.redis.keeper.exception;


public class RedisKeeperBadRequestException extends RedisKeeperRuntimeException {

  public RedisKeeperBadRequestException(String str) {
    super(str);
  }
}
