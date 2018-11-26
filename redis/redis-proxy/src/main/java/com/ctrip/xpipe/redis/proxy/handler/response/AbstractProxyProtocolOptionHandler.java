package com.ctrip.xpipe.redis.proxy.handler.response;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.core.proxy.exception.XPipeProxyResultException;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.IntSupplier;

public abstract class AbstractProxyProtocolOptionHandler implements ProxyProtocolOptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(ProxyProtocolOptionHandler.class);

    private IntSupplier longTimeInterval;

    protected AbstractProxyProtocolOptionHandler(IntSupplier longTimeInterval) {
        this.longTimeInterval = longTimeInterval;
    }

    @Override
    public void handle(Channel channel, String[] content) {
        long start = System.currentTimeMillis();
        logger.debug("[handle][start] {}", content == null ? "null" : content);
        doHandle(channel, removeHeaderIfNeeded(content));
        long duration = System.currentTimeMillis() - start;
        logger.debug("[handle][after] {}; duration: {}", content, duration);

        if(duration > longTimeInterval.getAsInt()) {
            logger.warn("[handle] {}", content, duration);
            EventMonitor.DEFAULT.logAlertEvent(String.format("%s; duration: %d", StringUtil.join(" ", content), duration));
        }
    }

    private String[] removeHeaderIfNeeded(String[] content) {
        if(content == null || content.length == 0) {
            throw new XPipeProxyResultException("empty protocol");
        }
        if(content[0].equalsIgnoreCase(getOption().toString())) {
            if(content.length == 1) {
                return new String[0];
            }
            String[] newContent = new String[content.length - 1];
            System.arraycopy(content, 1, newContent, 0, newContent.length);
            return newContent;
        }
        return content;
    }

    protected abstract void doHandle(Channel channel, String[] content);

    protected interface Responser {

        // synchonized method
        void response(Channel channel);
    }
}
