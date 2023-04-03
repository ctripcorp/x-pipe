package com.ctrip.xpipe.server;


import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午3:27:08
 */
public abstract class AbstractServer extends AbstractLifecycleObservable implements Server{
    protected static LoggingHandler debugLoggingHandler = new LoggingHandler(LogLevel.DEBUG);

    protected static LoggingHandler infoLoggingHandler = new LoggingHandler(LogLevel.INFO);
	
}
