package com.ctrip.xpipe.redis.keeper.handler;

import com.ctrip.xpipe.redis.core.proxy.DefaultProxyProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.core.proxy.ProxyProtocolParser;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author chen.zhu
 * <p>
 * May 04, 2018
 */
public class ProxyCommandHandler extends AbstractCommandHandler {

    private static final String WHITE_SPACE = " ";

    @Override
    protected void doHandle(String[] args, RedisClient redisClient) {
        String proxyProtocol = reStructCommand(args);
        logger.info("[doHandle]receive proxy protocol: {}", proxyProtocol);

        ProxyProtocolParser parser = new DefaultProxyProtocolParser();
        ProxyProtocol protocol = parser.read(proxyProtocol);

        fakeOne(protocol, redisClient);
    }

    private String reStructCommand(String[] args) {
        return StringUtil.join(WHITE_SPACE, args);
    }

    @Override
    public String[] getCommands() {
        return new String[]{"proxy"};
    }

    private void fakeOne(ProxyProtocol proxyProtocol, RedisClient redisClient) {


    }
}
