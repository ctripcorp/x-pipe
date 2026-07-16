package com.ctrip.xpipe.redis.core.client;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.netty.commands.AsyncNettyClient;
import com.ctrip.xpipe.netty.commands.ByteBufReceiver;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.RedisNettyClient;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.utils.ChannelUtil;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class RedisAsyncNettyClient extends AsyncNettyClient implements RedisNettyClient {

    protected Logger logger = LoggerFactory.getLogger(RedisAsyncNettyClient.class);

    private String clientName;

    private AsyncConnectionCondition asyncConnectionCondition;

    private static final String CLIENT_SET_NAME = "CLIENT SETNAME ";

    private static final String EXPECT_RESP = "OK";

    private static final int DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI = 660;

    private final AtomicBoolean doAfterConnectedSuccess = new AtomicBoolean(false);

    private final AtomicBoolean doAfterConnectedOver = new AtomicBoolean(false);

    private final CompletableFuture<Boolean> afterConnectedFuture = new CompletableFuture<>();

    public RedisAsyncNettyClient(ChannelFuture future, Endpoint endpoint, String clientName, AsyncConnectionCondition asyncConnectionCondition) {
        super(future, endpoint);
        this.clientName = clientName;
        this.asyncConnectionCondition = asyncConnectionCondition;
        future.addListener(new GenericFutureListener<Future<? super Void>>() {
            @Override
            public void operationComplete(Future<? super Void> future) throws Exception {
                if (future.isSuccess()) {
                    doAfterConnected();
                } else {
                    finishAfterConnected(false);
                }
            }
        });
    }

    protected void doAfterConnected() {
        if (asyncConnectionCondition != null && !asyncConnectionCondition.shouldDo()) {
            finishAfterConnected(true);
            return;
        }
        RequestStringParser requestString = new RequestStringParser(CLIENT_SET_NAME, clientName);
        SimpleStringParser simpleStringParser = new SimpleStringParser();
        Transaction transaction = Cat.newTransaction("netty.client.setName", clientName);
        try {
            RedisAsyncNettyClient.super.sendRequest(requestString.format(), new ByteBufReceiver() {
                @Override
                public RECEIVER_RESULT receive(Channel channel, ByteBuf byteBuf) {
                    try{
                        transaction.addData("remoteAddress", ChannelUtil.getSimpleIpport(channel.remoteAddress()));
                        transaction.addData("commandTimeoutMills", getAfterConnectCommandTimeoutMill());
                        RedisClientProtocol<String> payload = simpleStringParser.read(byteBuf);
                        String result = null;
                        if(payload != null){
                            result = payload.getPayload();
                        }
                        if(result == null){
                            return RECEIVER_RESULT.CONTINUE;
                        }
                        if (EXPECT_RESP.equalsIgnoreCase(result)){
                            transaction.setStatus(Transaction.SUCCESS);
                            logger.info("[redisAsync][clientSetName][success][{}] {}", desc, result);
                            finishAfterConnected(true);
                        } else {
                            transaction.setStatus(new XpipeException(String.format("[redisAsync][clientSetName][wont-result][%s] result:%s", desc, result)));
                            logger.warn("[redisAsync][clientSetName][err-result][{}] {}", desc, result);
                            finishAfterConnected(false);
                        }
                        transaction.complete();
                    }catch(Throwable th){
                        finishAfterConnected(false);
                        transaction.setStatus(th);
                        transaction.complete();
                        logger.error("[logTransaction]" + "netty.client.setName" + "," + clientName, th);
                        throw th;
                    }
                    return RECEIVER_RESULT.SUCCESS;
                }

                @Override
                public void clientClosed(NettyClient nettyClient) {
                    logger.warn("[redisAsync][clientSetName][wont-send][{}] {}", desc, nettyClient.channel());
                    transaction.setStatus(new XpipeException(String.format("[redisAsync][clientSetName][wont-send][%s]client closed %s", desc, nettyClient.channel())));
                    transaction.complete();
                    finishAfterConnected(false);
                }

                @Override
                public void clientClosed(NettyClient nettyClient, Throwable th) {
                    logger.warn("[redisAsync][clientSetName][wont-send][{}] {}", desc, nettyClient.channel(), th);
                    transaction.setStatus(th);
                    transaction.complete();
                    finishAfterConnected(false);
                }
            });
        } catch (Exception e) {
            transaction.setStatus(e);
            transaction.complete();
            logger.error("[redisAsync][clientSetName] err", e);
            finishAfterConnected(false);
        }

    }

    private void finishAfterConnected(boolean success) {
        if (doAfterConnectedOver.compareAndSet(false, true)) {
            doAfterConnectedSuccess.set(success);
            afterConnectedFuture.complete(success);
            if (!success && channel != null) {
                channel.close();
            }
        }
    }

    @Override
    public void sendRequest(ByteBuf byteBuf) {
        if (doAfterConnectedOver.get()) {
            if (doAfterConnectedSuccess.get()) {
                super.sendRequest(byteBuf);
            } else {
                logger.warn("[async][wont-send][afterConnected-fail][{}]", desc);
            }
            return;
        }
        afterConnectedFuture.whenComplete((success, throwable) -> {
            if (Boolean.TRUE.equals(success)) {
                RedisAsyncNettyClient.super.sendRequest(byteBuf);
            } else {
                logger.warn("[async][wont-send][afterConnected-fail][{}]", desc);
            }
        });
    }

    @Override
    public void sendRequest(ByteBuf byteBuf, ByteBufReceiver byteBufReceiver) {
        if (doAfterConnectedOver.get()) {
            if (doAfterConnectedSuccess.get()) {
                super.sendRequest(byteBuf, byteBufReceiver);
            } else {
                logger.warn("[async][wont-send][afterConnected-fail][{}] {}", desc, byteBufReceiver.getClass().getSimpleName());
                byteBufReceiver.clientClosed(this);
            }
            return;
        }
        afterConnectedFuture.whenComplete((success, throwable) -> {
            if (Boolean.TRUE.equals(success)) {
                RedisAsyncNettyClient.super.sendRequest(byteBuf, byteBufReceiver);
            } else {
                logger.warn("[async][wont-send][afterConnected-fail][{}] {}", desc, byteBufReceiver.getClass().getSimpleName());
                byteBufReceiver.clientClosed(RedisAsyncNettyClient.this);
            }
        });
    }

    @Override
    public boolean getDoAfterConnectedOver() {
        return doAfterConnectedOver.get();
    }

    @Override
    public boolean getDoAfterConnectedSuccess() {
        return doAfterConnectedSuccess.get();
    }

    @Override
    public int getAfterConnectCommandTimeoutMill() {
        return DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI;
    }

}
