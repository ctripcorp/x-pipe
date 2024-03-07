package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractConfigCommand;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.handler.AbstractCommandHandler;

/**
 * @author lishanglin
 * date 2024/3/5
 */
public class ConfigHandler extends AbstractCommandHandler {

    @Override
    protected void doHandle(String[] args, RedisClient<?> redisClient) throws Exception {

        if (args.length > 1) {
            if (args[0].equalsIgnoreCase("get")) {
                String param = args[1];
                if (param.equalsIgnoreCase(AbstractConfigCommand.REDIS_CONFIG_TYPE.RORDB_SYNC.getConfigName())) {
                    final RedisKeeperServer redisKeeperServer = (RedisKeeperServer) redisClient.getRedisServer();
                    if (null != redisKeeperServer.getRedisMaster()) {
                        redisKeeperServer.getRedisMaster().checkMasterSupportRordb().addListener(commandFuture -> {
                            redisClient.sendMessage(new ArrayParser(new Object[] {
                                    AbstractConfigCommand.REDIS_CONFIG_TYPE.RORDB_SYNC.getConfigName(),
                                    (commandFuture.isSuccess() && commandFuture.get()) ? "yes" : "no"
                            }).format());
                        });
                    } else {
                        redisClient.sendMessage("*0\r\n".getBytes());
                    }
                } else {
                    redisClient.sendMessage("*0\r\n".getBytes());
                }
            } else {
                throw new IllegalStateException("unknown command:" + args[0]);
            }
        }

    }

    @Override
    public String[] getCommands() {
        return new String[]{"config"};
    }
}
