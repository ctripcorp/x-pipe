package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisMultiKeyOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author TB
 * @date 2026/3/9 16:08
 */
public class TransactionAsyncCommand extends TransactionCommand{

    public static String ERR_GTID_COMMAND_EXECUTED = "ERR gtid command is executed";

    private AsyncRedisClient client;

    private Set<RedisKey> redisKeys;

    private Object resource;

    public TransactionAsyncCommand(AsyncRedisClient client){
        super();
        this.client = client;
        this.redisKeys = new HashSet<>();
    }

    public void addTransactionCommands(RedisOpCommand<?> redisOpCommand, long commandOffset, String gtid) {
        if(redisOpCommand instanceof MultiDataCommand){
            MultiDataCommand multiDataCommand = (MultiDataCommand) redisOpCommand;
            redisKeys.addAll(multiDataCommand.keys());
        }else if(redisOpCommand instanceof DefaultDataCommand){
            DefaultDataCommand defaultDataCommand = (DefaultDataCommand) redisOpCommand;
            redisKeys.add(defaultDataCommand.key());
        }
        super.addTransactionCommands(redisOpCommand,commandOffset,gtid);
    }

    @Override
    protected void doExecute() throws Throwable{
        List<Object[]> multiRawArgs = new ArrayList<>();

        for(RedisOpCommand redisOpCommand:transactionCommands){
            if(redisOpCommand instanceof MultiDataCommand){
                MultiDataCommand multiDataCommand = (MultiDataCommand) redisOpCommand;
                if(resource == null){
                    resource = client.select(multiDataCommand.keys().get(0).get());
                }
                Object[] selectArgs = new byte[][]{"select".getBytes(),(multiDataCommand.getDbNumber()+"").getBytes()};
                Object[] rawArgs = multiDataCommand.redisOp().buildRawOpArgs();
                multiRawArgs.add(selectArgs);
                multiRawArgs.add(rawArgs);
            }else if(redisOpCommand instanceof DefaultDataCommand){
                DefaultDataCommand defaultDataCommand = (DefaultDataCommand) redisOpCommand;
                if(resource == null) {
                    resource = client.select(defaultDataCommand.key().get());
                }
                Object[] selectArgs = new byte[][]{"select".getBytes(),(defaultDataCommand.getDbNumber()+"").getBytes()};
                multiRawArgs.add(selectArgs);
                multiRawArgs.add(defaultDataCommand.redisOp().buildRawOpArgs());
            }
        }

        long startTime = System.nanoTime();

        client
                .writeMulti(resource, 0, multiRawArgs.toArray())
                .addListener(f -> {
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("[command] write key {} end, total time {}", redisOp() instanceof RedisSingleKeyOp ? ((RedisSingleKeyOp) redisOp()).getKey() : (redisOp() instanceof RedisMultiKeyOp ? keys() : "none"), System.nanoTime() - startTime);
                    }
                    if (f.isSuccess()) {
                        future().setSuccess(true);
                    } else {
                        if (f.cause().getMessage().startsWith(ERR_GTID_COMMAND_EXECUTED)) {
                            future().setSuccess(true);
                        } else {
                            future().setFailure(f.cause());
                        }
                    }
                });
    }

    public boolean validTransaction(){
        if(redisKeys.size() == 1) return true;
        for(RedisKey redisKey:redisKeys) {
            byte[] tag = client.hashTag(redisKey.get());
            return tag != null;
        }
        return false;
    }
}
