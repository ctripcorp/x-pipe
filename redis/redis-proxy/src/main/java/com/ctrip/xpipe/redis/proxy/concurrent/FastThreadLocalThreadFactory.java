package com.ctrip.xpipe.redis.proxy.concurrent;

import com.ctrip.xpipe.utils.XpipeThreadFactory;
import io.netty.util.concurrent.FastThreadLocalThread;

import java.util.concurrent.ThreadFactory;

/**
 * @author chen.zhu
 * <p>
 * Jul 10, 2018
 */
public final class FastThreadLocalThreadFactory extends XpipeThreadFactory {

    private final static ThreadGroup threadGroup = new ThreadGroup("XPipe-Fast");

    private FastThreadLocalThreadFactory(String namePrefix, boolean daemon) {
        super(namePrefix, daemon);
    }

    public static ThreadFactory create(String namePrefix, boolean daemon) {
        return new FastThreadLocalThreadFactory(namePrefix, daemon);
    }

    public static ThreadFactory create(String namePrefix) {
        return create(namePrefix, false);
    }


    @Override
    public Thread newThread(Runnable r) {
        Thread t = new FastThreadLocalThread(threadGroup, r,//
                m_namePrefix + "-" + m_threadNumber.getAndIncrement());
        t.setDaemon(m_daemon);
        if (t.getPriority() != Thread.NORM_PRIORITY)
            t.setPriority(Thread.NORM_PRIORITY);
        return t;
    }

}
