package com.ctrip.xpipe.redis.console.health.info;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chen.zhu
 * <p>
 * Sep 20, 2017
 */

/*
* Used to Test whether Redis with version 2.8.19 is in use
* If so, alert admin if Redis is in Recovery Site
* Otherwise, alert admin if "repl-diskless-sync" option is opened*/
public interface InfoCallback {

    Logger logger = LoggerFactory.getLogger(InfoCallback.class.getSimpleName());

    String version(String info);

    void fail(Throwable th);
}
