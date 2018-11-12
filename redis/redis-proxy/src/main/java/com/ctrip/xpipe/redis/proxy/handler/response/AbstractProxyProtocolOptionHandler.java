package com.ctrip.xpipe.redis.proxy.handler.response;

import io.netty.channel.Channel;

public abstract class AbstractProxyProtocolOptionHandler implements ProxyProtocolOptionHandler {


    @Override
    public void handle(Channel channel, String[] content) {
        doHandle(channel, removeHeaderIfNeeded(content));
    }

    private String[] removeHeaderIfNeeded(String[] content) {
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
        void response(Channel channel);
    }
}
