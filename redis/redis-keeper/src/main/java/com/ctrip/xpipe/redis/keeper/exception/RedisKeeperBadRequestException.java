package com.ctrip.xpipe.redis.keeper.exception;


public class RedisKeeperBadRequestException extends RedisKeeperRuntimeException {

	private static final long serialVersionUID = 1L;

public RedisKeeperBadRequestException(String str) {
    super(str);
  }
}
