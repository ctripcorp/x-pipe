package com.ctrip.xpipe.redis.core.proxy;


import com.ctrip.xpipe.redis.core.proxy.parser.ProxyOptionParser;
import com.ctrip.xpipe.redis.core.proxy.parser.UnknownOptionParser;
import com.ctrip.xpipe.redis.core.proxy.parser.path.ForwardForOptionParser;
import com.ctrip.xpipe.redis.core.proxy.parser.route.RouteOptionParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chen.zhu
 * <p>
 * May 04, 2018
 */
public enum PROXY_OPTION {

    ROUTE{
        @Override
        protected ProxyOptionParser getProxyOptionParser() {
            return new RouteOptionParser();
        }
    },
    FORWARD_FOR {
        @Override
        protected ProxyOptionParser getProxyOptionParser() {
            return new ForwardForOptionParser();
        }
    },
    UNKOWN {
        @Override
        protected ProxyOptionParser getProxyOptionParser() {
            return new UnknownOptionParser();
        }
    };

    public static ProxyOptionParser parse(String option) {
        String optionType = option.split("\\h")[0].trim().toUpperCase();
        PROXY_OPTION proxyOption;
        try {
            proxyOption = PROXY_OPTION.valueOf(optionType);
        } catch (IllegalArgumentException e) {
            logger.info("[parse] unkown option: {}", option);
            proxyOption = PROXY_OPTION.UNKOWN;
        }
        return proxyOption.getProxyOptionParser().read(option);
    }

    private static Logger logger = LoggerFactory.getLogger(PROXY_OPTION.class);

    protected abstract ProxyOptionParser getProxyOptionParser();
}
