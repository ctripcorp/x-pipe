package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisMultiKeyOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * @author TB
 * @date 2026/3/9 16:08
 */
public class TransactionAsyncCommand extends TransactionCommand{

    public static String ERR_GTID_COMMAND_EXECUTED = "ERR gtid command is executed";

    private AsyncRedisClient client;

    private Set<RedisKey> redisKeys;

    private Object resource;

    final ExecutorService workThreads;

    public TransactionAsyncCommand(AsyncRedisClient client,ExecutorService workThreads){
        super();
        this.client = client;
        this.redisKeys = new HashSet<>();
        this.workThreads = workThreads;
    }

    public void addTransactionCommands(RedisOpCommand<?> redisOpCommand, long commandOffset, String gtid) {
        super.addTransactionCommands(redisOpCommand,commandOffset,gtid);
    }

    @Override
    protected void doExecute() throws Throwable{
        Map<Object,List<byte[][]>> resourceMultiRawArgs = new HashMap<>();
        for(RedisOpCommand redisOpCommand:transactionCommands){
            if(redisOpCommand instanceof MultiDataCommand){
                MultiDataCommand multiDataCommand = (MultiDataCommand) redisOpCommand;
                List<Object> keys = multiDataCommand.keys().stream().map(RedisKey::get).collect(Collectors.toList());
                Map<Object,List<Object>> resourceOps = client.selectMulti(keys);
                for(Map.Entry<Object,List<Object>> entry:resourceOps.entrySet()){
                    List<byte[][]> args = resourceMultiRawArgs.computeIfAbsent(entry.getKey(),(key)-> new ArrayList<>());
                    if(multiDataCommand.getDbNumber() != 0) {
                        byte[][] selectArgs = new byte[][]{"select".getBytes(), (multiDataCommand.getDbNumber() + "").getBytes()};
                        args.add(selectArgs);
                    }
                    args.add(multiDataCommand.redisOpAsMulti().subOp(entry.getValue().stream().map(keys::indexOf).collect(Collectors.toSet())).buildRawOpArgs());
                }
            }else if(redisOpCommand instanceof DefaultDataCommand){
                DefaultDataCommand defaultDataCommand = (DefaultDataCommand) redisOpCommand;
                Object  resource = client.select(defaultDataCommand.key().get());
                List<byte[][]> args = resourceMultiRawArgs.computeIfAbsent(resource,(key)-> new ArrayList<>());
                if(defaultDataCommand.getDbNumber() != 0) {
                    byte[][] selectArgs = new byte[][]{"select".getBytes(), (defaultDataCommand.getDbNumber() + "").getBytes()};
                    args.add(selectArgs);
                }
                args.add(defaultDataCommand.redisOp().buildRawOpArgs());
            }
        }

        ParallelCommandChain parallelCommandChain = new ParallelCommandChain(workThreads, false);

        for(Map.Entry<Object,List<byte[][]>> entry:resourceMultiRawArgs.entrySet()){
            parallelCommandChain.add(new DefaultRawCommand(client,entry.getKey(),entry.getValue()));
        }
        parallelCommandChain.execute().addListener(commandFuture -> {
            if (commandFuture.isSuccess()) {
                TransactionAsyncCommand.this.future().setSuccess(true);
            } else {
                TransactionAsyncCommand.this.future().setFailure(commandFuture.cause());
            }
        });
    }
}
