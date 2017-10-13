package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;
import com.ctrip.xpipe.redis.core.protocal.pojo.RedisInfo;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *         <p>
 *         Sep 07, 2017
 */
public class InfoReplicationComplementCommand extends AbstractCommand<RedisInfo> {

    private SimpleObjectPool<NettyClient> clientPool;
    private ScheduledExecutorService scheduled;

    public InfoReplicationComplementCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled){
        this.clientPool = clientPool;
        this.scheduled = scheduled;
    }


    @Override
    public String getName() {
        return "InfoReplicationComplementCommand";
    }

    @Override
    protected void doExecute() throws InterruptedException, ExecutionException {

        new InfoReplicationCommand(clientPool, scheduled).execute().addListener(new CommandFutureListener<RedisInfo>() {
            @Override
            public void operationComplete(CommandFuture<RedisInfo> commandFuture) throws InterruptedException, ExecutionException {

                if(!commandFuture.isSuccess()){
                    future().setFailure(commandFuture.cause());
                    return;
                }

                RedisInfo redisInfo = commandFuture.get();
                if(redisInfo instanceof MasterInfo){
                    MasterInfo masterInfo = (MasterInfo) redisInfo;
                    if (masterInfo.getReplId() == null){
                        logger.info("replid null, get master id. {}, {}", clientPool.desc(), masterInfo);
                        getRunId(masterInfo);
                    }else {
                        future().setSuccess(redisInfo);
                    }
                }else {
                    future().setSuccess(redisInfo);
                }
            }
        });
    }

    private void getRunId(MasterInfo masterInfo) {
        new InfoCommand(clientPool, InfoCommand.INFO_TYPE.SERVER, scheduled).execute().addListener(new CommandFutureListener<String>() {

            @Override
            public void operationComplete(CommandFuture<String> commandFuture) throws InterruptedException, ExecutionException {

                if(!commandFuture.isSuccess()){
                    logger.info("[getRunId][fail use previous result]{}, {}", clientPool.desc(), masterInfo);
                    future().setSuccess(masterInfo);
                    return;
                }
                String serverInfo = commandFuture.get();
                masterInfo.setReplId(getMasterId(serverInfo));
                future().setSuccess(masterInfo);
            }
        });
    }

    private String getMasterId(String serverInfo) {

        for(String line : serverInfo.split("\\s+")){
            String []sp = line.split("\\s*:\\s*");
            if(sp.length != 2){
                continue;
            }
            if(sp[0].trim().equalsIgnoreCase("run_id")){
                return sp[1].trim();
            }
        }
        return null;
    }

    @Override
    protected void doReset() {

    }
}
