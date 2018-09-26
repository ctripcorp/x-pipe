package com.ctrip.xpipe.redis.proxy.session;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Sep 25, 2018
 */
public class SessionWritableEventHandler implements SessionEventHandler {

    private Session session;

    private ScheduledFuture closeTask;

    private ScheduledExecutorService schedued;

    private ProxyConfig proxyConfig;

    public SessionWritableEventHandler(Session session, ScheduledExecutorService schedued, ProxyConfig proxyConfig) {
        this.session = session;
        this.schedued = schedued;
        this.proxyConfig = proxyConfig;
    }

    @Override
    public void onInit() {

    }

    @Override
    public void onEstablished() {
        cleanup();
    }

    @Override
    public void onWritable() {
        cleanup();
    }

    @Override
    public void onNotWritable() {
        cleanup();
        closeTask = schedued.schedule(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                logger.info("[onNotWritable] close session as not writable for {} milli-sec",
                        proxyConfig.getCloseChannelAfterReadCloseMilli());
                session.release();
            }
        }, proxyConfig.getCloseChannelAfterReadCloseMilli(), TimeUnit.MILLISECONDS);
    }

    private void cleanup() {
        if(closeTask != null) {
            closeTask.cancel(true);
        }
        closeTask = null;
    }
}
