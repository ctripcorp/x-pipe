package com.ctrip.xpipe.redis.console.health;

/**
 * @author marsqing
 *
 *         Dec 6, 2016 4:57:30 PM
 */
public interface PingCallback {

	void pong(String pongMsg);

	void fail(Throwable th);

}
