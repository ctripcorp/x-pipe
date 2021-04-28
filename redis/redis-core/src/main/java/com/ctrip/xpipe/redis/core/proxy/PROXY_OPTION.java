package com.ctrip.xpipe.redis.core.proxy;


import com.ctrip.xpipe.redis.core.proxy.parser.ProxyOptionParser;
import com.ctrip.xpipe.redis.core.proxy.parser.UnknownOptionParser;
import com.ctrip.xpipe.redis.core.proxy.parser.content.DefaultProxyContentParser;
import com.ctrip.xpipe.redis.core.proxy.parser.monitor.MonitorOptionParser;
import com.ctrip.xpipe.redis.core.proxy.parser.path.ForwardForOptionParser;
import com.ctrip.xpipe.redis.core.proxy.parser.ping.PingOptionParser;
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
        public ProxyOptionParser getProxyOptionParser() {
            return new RouteOptionParser();
        }

        @Override
        public boolean hasResponse() {
            return false;
        }
    },
    FORWARD_FOR {
        @Override
        public ProxyOptionParser getProxyOptionParser() {
            return new ForwardForOptionParser();
        }

        @Override
        public boolean hasResponse() {
            return false;
        }
    },
    UNKOWN {
        @Override
        public ProxyOptionParser getProxyOptionParser() {
            return new UnknownOptionParser();
        }

        @Override
        public boolean hasResponse() {
            return false;
        }
    },
    PING {
        @Override
        public ProxyOptionParser getProxyOptionParser() {
            return new PingOptionParser();
        }

        @Override
        public boolean hasResponse() {
            return true;
        }
    },
    MONITOR {
        @Override
        public ProxyOptionParser getProxyOptionParser() {
            return new MonitorOptionParser();
        }

        @Override
        public boolean hasResponse() {
            return true;
        }
    },
    CONTENT {
        @Override
        public ProxyOptionParser getProxyOptionParser() {
            return new DefaultProxyContentParser();
        }

        @Override
        public boolean hasResponse() {
            return false;
        }
    };

    public static ProxyOptionParser getOptionParser(String option) {
        return parse(option).getProxyOptionParser().read(option);
    }

    public static PROXY_OPTION parse(String option) {
        String optionType = option.split("\\h")[0].trim().toUpperCase();
        PROXY_OPTION proxyOption;
        try {
            proxyOption = PROXY_OPTION.valueOf(optionType);
        } catch (IllegalArgumentException e) {
            logger.info("[getOptionParser] unkown option: {}", option);
            proxyOption = PROXY_OPTION.UNKOWN;
        }
        return proxyOption;
    }

    private static Logger logger = LoggerFactory.getLogger(PROXY_OPTION.class);

    public abstract ProxyOptionParser getProxyOptionParser();

    public abstract boolean hasResponse();
}
