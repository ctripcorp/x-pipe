package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractConfigCommand;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.handler.AbstractCommandHandler;

/**
 * @author lishanglin
 * date 2024/3/5
 */
public class ConfigHandler extends AbstractCommandHandler {

    private static final String EMPTY_ARRAY = "*0\r\n";

    private static final String FLAG_ON = "1";

    private static final String FLAG_OFF = "0";

    @Override
    protected void doHandle(String[] args, RedisClient<?> redisClient) throws Exception {

        if (args.length > 1) {
            if (args[0].equalsIgnoreCase("get")) {
                handleConfigGet(args[1], redisClient);
            } else {
                throw new IllegalStateException("unknown command:" + args[0]);
            }
        }

    }

    private void handleConfigGet(String param, RedisClient<?> redisClient) {
        if (param.equalsIgnoreCase(AbstractConfigCommand.REDIS_CONFIG_TYPE.RORDB_SYNC.getConfigName())) {
            replyRordbSync(redisClient);
            return;
        }
        RedisKeeperServer redisKeeperServer = (RedisKeeperServer) redisClient.getRedisServer();
        if (param.equalsIgnoreCase(AbstractConfigCommand.REDIS_CONFIG_TYPE.PREPARE_WATCH.getConfigName())) {
            replyFlag(redisClient, AbstractConfigCommand.REDIS_CONFIG_TYPE.PREPARE_WATCH.getConfigName(),
                    isPrepareWatchCapable(redisKeeperServer));
            return;
        }
        if (param.equalsIgnoreCase(AbstractConfigCommand.REDIS_CONFIG_TYPE.PUBSUB_PARSE.getConfigName())) {
            replyFlag(redisClient, AbstractConfigCommand.REDIS_CONFIG_TYPE.PUBSUB_PARSE.getConfigName(),
                    isPubsubParseCapable(redisKeeperServer));
            return;
        }
        redisClient.sendMessage(EMPTY_ARRAY.getBytes());
    }

    private void replyRordbSync(RedisClient<?> redisClient) {
        final RedisKeeperServer redisKeeperServer = (RedisKeeperServer) redisClient.getRedisServer();
        if (null != redisKeeperServer.getRedisMaster()) {
            redisKeeperServer.getRedisMaster().checkMasterSupportRordb().addListener(commandFuture -> {
                redisClient.sendMessage(new ArrayParser(new Object[] {
                        AbstractConfigCommand.REDIS_CONFIG_TYPE.RORDB_SYNC.getConfigName(),
                        (commandFuture.isSuccess() && commandFuture.get()) ? "yes" : "no"
                }).format());
            });
        } else {
            redisClient.sendMessage(EMPTY_ARRAY.getBytes());
        }
    }

    /**
     * Capability: if this Keeper becomes PREPARE, can it hang slaves (D20).
     * Independent of current keeper state.
     */
    private static boolean isPrepareWatchCapable(RedisKeeperServer server) {
        KeeperConfig config = server.getKeeperConfig();
        return config != null && config.isPrepareStoreWatchEnabled() && server.isTfsMode();
    }

    private static boolean isPubsubParseCapable(RedisKeeperServer server) {
        KeeperConfig config = server.getKeeperConfig();
        return config != null && config.isPubsubParseEnabled();
    }

    private static void replyFlag(RedisClient<?> redisClient, String key, boolean on) {
        redisClient.sendMessage(new ArrayParser(new Object[] { key, on ? FLAG_ON : FLAG_OFF }).format());
    }

    @Override
    public String[] getCommands() {
        return new String[]{"config"};
    }

    @Override
    public boolean support(RedisServer server) {
        return server instanceof RedisKeeperServer;
    }
}
