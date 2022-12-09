package com.ctrip.xpipe.redis.keeper.handler.applier;

import com.ctrip.framework.xpipe.redis.ProxyRegistry;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.api.proxy.ProxyProtocol;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisServer;
import com.ctrip.xpipe.redis.keeper.applier.ApplierServer;
import com.ctrip.xpipe.redis.keeper.handler.AbstractCommandHandler;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;

import java.util.Arrays;

import static com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser.WHITE_SPACE;

/**
 * @author lishanglin
 * date 2022/6/11
 */
public class ApplierCommandHandler extends AbstractCommandHandler {

    public final static String GET_STATE = "getstate";

    public final static String SET_STATE = "setstate";

    public final static String STATE_ACTIVE = "active";

    public final static String STATE_BACKUP = "backup";

    @Override
    public String[] getCommands() {
        return new String[]{"applier"};
    }

    @Override
    protected void doHandle(String[] args, RedisClient<?> redisClient) throws Exception {

        if(args.length >= 1) {

            if(args[0].equalsIgnoreCase(GET_STATE)){
                // TODO
            }else if(args[0].equalsIgnoreCase(SET_STATE)){
                if(args.length >= 2 && args[1].equalsIgnoreCase(STATE_BACKUP)) {
                    ((ApplierServer)redisClient.getRedisServer()).setStateBackup();
                    redisClient.sendMessage(new SimpleStringParser(RedisProtocol.OK).format());
                } else if (args.length >= 5 && args[1].equalsIgnoreCase(STATE_ACTIVE)) {
                    Endpoint upstreamEndpoint = getMasterAddress(args);
                    GtidSet gtidSet = new GtidSet(args[4]);
                    ((ApplierServer)redisClient.getRedisServer()).setStateActive(upstreamEndpoint, gtidSet);
                    redisClient.sendMessage(new SimpleStringParser(RedisProtocol.OK).format());
                } else{
                    throw new IllegalArgumentException("setstate argument error:" + StringUtil.join(" ", args));
                }
            }else{
                throw new IllegalStateException("unknown command:" + args[0]);
            }
        }
    }

    protected Endpoint getMasterAddress(String[] args) {
        String ip = args[2];
        int port = Integer.parseInt(args[3]);

        if (args.length >= 6 && ProxyProtocol.KEY_WORD.equalsIgnoreCase(args[5])) {
            ProxyConnectProtocol protocol = getProxyProtocol(args);
            ProxyRegistry.registerProxy(ip, port, protocol.getContent());
            return new DefaultEndPoint(ip, port, protocol);
        } else {
            ProxyRegistry.unregisterProxy(ip, port);
            return new DefaultEndPoint(ip, port);
        }
    }

    protected ProxyConnectProtocol getProxyProtocol(String[] args) {
        String[] protocolArgs = Arrays.copyOfRange(args, 5, args.length);
        String dstAddr = String.format("%s://%s:%s", args[args.length - 1], args[2], args[3]);
        protocolArgs[protocolArgs.length - 1] = dstAddr;
        String protocol = StringUtil.join(WHITE_SPACE, protocolArgs);
        logger.info("[getProxyProtocol] protocol: {}", protocol);
        return new DefaultProxyConnectProtocolParser().read(protocol);
    }

    @Override
    public boolean support(RedisServer server) {
        return server instanceof ApplierServer;
    }

}
