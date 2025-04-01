package com.ctrip.xpipe.redis.keeper.handler.applier;

import com.ctrip.framework.xpipe.redis.ProxyRegistry;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.api.proxy.ProxyProtocol;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.protocal.Sync;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisServer;
import com.ctrip.xpipe.redis.keeper.applier.ApplierConfig;
import com.ctrip.xpipe.redis.keeper.applier.ApplierServer;
import com.ctrip.xpipe.redis.keeper.handler.AbstractCommandHandler;
import com.ctrip.xpipe.utils.StringUtil;

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

    public final static String FREEZE_CONFIG = "freezeconfig";

    public final static String STOP_FREEZE_CONFIG = "stopfreezeconfig";

    public final static String GET_FREEZE_LAST = "getfreezelast";

    public final static String SYNC_PROTOCOL = "PROTOCOL";

    @Override
    public String[] getCommands() {
        return new String[]{"applier"};
    }

    @Override
    protected void doHandle(String[] args, RedisClient<?> redisClient) throws Exception {

        if (args.length >= 1) {

            if (args[0].equalsIgnoreCase(GET_STATE)) {
                // TODO
            } else if (args[0].equalsIgnoreCase(SET_STATE)) {
                if (args.length >= 2 && args[1].equalsIgnoreCase(STATE_BACKUP)) {
                    ((ApplierServer) redisClient.getRedisServer()).setStateBackup();
                    redisClient.sendMessage(new SimpleStringParser(RedisProtocol.OK).format());
                } else if (args.length >= 5 && args[1].equalsIgnoreCase(STATE_ACTIVE)) {
                    Endpoint upstreamEndpoint = getMasterAddress(args);
                    GtidSet gtidSet = new GtidSet(args[4]);
                    ApplierConfig config = parseConfig(args, 5);
                    ((ApplierServer) redisClient.getRedisServer()).setStateActive(upstreamEndpoint, gtidSet, config);
                    redisClient.sendMessage(new SimpleStringParser(RedisProtocol.OK).format());
                } else {
                    throw new IllegalArgumentException("setstate argument error:" + StringUtil.join(" ", args));
                }
            } else if (FREEZE_CONFIG.equalsIgnoreCase(args[0])) {
                ((ApplierServer) redisClient.getRedisServer()).freezeConfig();
                redisClient.sendMessage(new SimpleStringParser(RedisProtocol.OK).format());
            } else if (STOP_FREEZE_CONFIG.equalsIgnoreCase(args[0])) {
                ((ApplierServer) redisClient.getRedisServer()).stopFreezeConfig();
                redisClient.sendMessage(new SimpleStringParser(RedisProtocol.OK).format());
            } else if (GET_FREEZE_LAST.equalsIgnoreCase(args[0])) {
                long freezeLastMillis = ((ApplierServer) redisClient.getRedisServer()).getFreezeLastMillis();
                redisClient.sendMessage(new SimpleStringParser(Long.toString(freezeLastMillis)).format());
            } else {
                throw new IllegalStateException("unknown command:" + args[0]);
            }
        }
    }

    protected ApplierConfig parseConfig(String[] args, int startPos) {
        ApplierConfig config = new ApplierConfig();

        int i = startPos;
        while (i < args.length) {
            String opt = args[i].toUpperCase();
            if (opt.equals(SYNC_PROTOCOL)) {
                boolean useXsync = args[i + 1].equalsIgnoreCase(Sync.XSYNC);
                i += 2;
                config.setUseXsync(useXsync);
            } else if (opt.equals("DROP_KEYS_ALLOW")) {
                int keys = Integer.parseInt(args[i + 1]);
                i += 2;
                config.setDropAllowKeys(keys);
            } else if (opt.equals("DROP_KEYS_RATION_ALLOW")) {
                int ration = Integer.parseInt(args[i + 1]);
                i += 2;
                config.setDropAllowRation(ration);
            } else if (opt.equals("PROXY")) {
                // PROXY OPT only in the end of args
                break;
            } else {
                throw new IllegalStateException("unknown opt:" + args[i]);
            }
        }

        return config;
    }

    protected Endpoint getMasterAddress(String[] args) {
        String ip = args[2];
        int port = Integer.parseInt(args[3]);
        int index = findIndex(args, ProxyProtocol.KEY_WORD);
        if (index > -1) {
            ProxyConnectProtocol protocol = getProxyProtocol(args, index);
            ProxyRegistry.registerProxy(ip, port, protocol.getContent());
            return new DefaultEndPoint(ip, port, protocol);
        } else {
            ProxyRegistry.unregisterProxy(ip, port);
            return new DefaultEndPoint(ip, port);
        }
    }

    protected boolean useXsync(String[] args) {
        String protocol = Sync.XSYNC;
        int index = findIndex(args, SYNC_PROTOCOL);
        if (index > -1) {
            protocol = args[index + 1];
        }
        return protocol.equalsIgnoreCase(Sync.XSYNC);
    }

    protected int findIndex(String[] args, String target) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase(target)) {
                return i;
            }
        }
        return -1;
    }

    protected ProxyConnectProtocol getProxyProtocol(String[] args, int startIndex) {
        String[] protocolArgs = Arrays.copyOfRange(args, startIndex, args.length);
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
