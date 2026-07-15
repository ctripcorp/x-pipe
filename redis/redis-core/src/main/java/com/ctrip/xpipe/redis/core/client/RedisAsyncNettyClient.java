package com.ctrip.xpipe.redis.core.client;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.netty.commands.AsyncNettyClient;
import com.ctrip.xpipe.netty.commands.ByteBufReceiver;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.RedisNettyClient;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.utils.ChannelUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class RedisAsyncNettyClient extends AsyncNettyClient implements RedisNettyClient {

    protected Logger logger = LoggerFactory.getLogger(RedisAsyncNettyClient.class);

    private String clientName;

    private AsyncConnectionCondition asyncConnectionCondition;

    private static final String CLIENT_SET_NAME = "CLIENT SETNAME ";

    private static final String EXPECT_RESP = "OK";

    private static final int DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI = 660;

    private boolean doAfterConnectedSuccess = false;

    protected final AtomicReference<Boolean> doAfterConnectedOver = new AtomicReference<>(false);

    public RedisAsyncNettyClient(ChannelFuture future, Endpoint endpoint, String clientName, AsyncConnectionCondition asyncConnectionCondition) {
        super(future, endpoint);
        this.clientName = clientName;
        this.asyncConnectionCondition = asyncConnectionCondition;
        future.addListener(new GenericFutureListener<Future<? super Void>>() {
            @Override
            public void operationComplete(Future<? super Void> future) throws Exception {
                if (future.isSuccess()) {
                    doAfterConnected();
                    doAfterConnectedOver.set(true);
                }
            }
        });
    }

    protected void doAfterConnected() {
        if (asyncConnectionCondition != null && !asyncConnectionCondition.shouldDo()) {
            return;
        }
        RequestStringParser requestString = new RequestStringParser(CLIENT_SET_NAME, clientName);
        SimpleStringParser simpleStringParser = new SimpleStringParser();
        try {
            RedisAsyncNettyClient.super.sendRequest(requestString.format(), new ByteBufReceiver() {
                @Override
                public RECEIVER_RESULT receive(Channel channel, ByteBuf byteBuf) {
                    try{
                        RedisClientProtocol<String> payload = simpleStringParser.read(byteBuf);
                        String result = null;
                        if(payload != null){
                            result = payload.getPayload();
                        }
                        if(result == null){
                            return RECEIVER_RESULT.CONTINUE;
                        }
                        if (EXPECT_RESP.equalsIgnoreCase(result)){
                            doAfterConnectedSuccess = true;
                            EventMonitor.DEFAULT.logEvent("netty.client.setName", "success");
                            logger.info("[redisAsync][clientSetName][{}][success] result:{}", desc, result);
                        } else {
                            doAfterConnectedSuccess = false;
                            EventMonitor.DEFAULT.logError("netty.client.setName", "fail",
                                    Map.of("remote", ChannelUtil.getSimpleIpport(channel.remoteAddress()),
                                            "result", result));
                            logger.error("[redisAsync][clientSetName][{}][fail] err-result result:{}", desc, result);
                        }
                    }catch(Throwable th){
                        doAfterConnectedSuccess = false;
                        EventMonitor.DEFAULT.logError("netty.client.setName", "fail",
                                Map.of("remote", ChannelUtil.getSimpleIpport(channel.remoteAddress())));
                        logger.error("[redisAsync][clientSetName][{}][fail] receive error", desc, th);
                        throw th;
                    }
                    return RECEIVER_RESULT.SUCCESS;
                }

                @Override
                public void clientClosed(NettyClient nettyClient) {
                    if (doAfterConnectedOver.get()) {
                        EventMonitor.DEFAULT.logError("netty.client.setName", "fail",
                                Map.of("remote", ChannelUtil.getSimpleIpport(nettyClient.channel().remoteAddress())));
                        logger.error("[redisAsync][clientSetName][{}][fail] no-response channel:{}", desc, nettyClient.channel());
                    } else {
                        EventMonitor.DEFAULT.logError("netty.client.setName", "wont-send");
                        logger.error("[redisAsync][clientSetName][{}][wont-send] channel:{}", desc, nettyClient.channel());
                    }
                }

                @Override
                public void clientClosed(NettyClient nettyClient, Throwable th) {
                    if (doAfterConnectedOver.get()) {
                        EventMonitor.DEFAULT.logError("netty.client.setName", "fail",
                                Map.of("remote", ChannelUtil.getSimpleIpport(nettyClient.channel().remoteAddress())));
                        logger.error("[redisAsync][clientSetName][{}][fail] no-response channel:{}", desc, nettyClient.channel(), th);
                    } else {
                        EventMonitor.DEFAULT.logError("netty.client.setName", "wont-send");
                        logger.error("[redisAsync][clientSetName][{}][wont-send] channel:{}", desc, nettyClient.channel(), th);
                    }
                }
            });
        } catch (Exception e) {
            EventMonitor.DEFAULT.logError("netty.client.setName", "fail");
            logger.error("[redisAsync][clientSetName][{}][fail] send error", desc, e);
        }

    }

    @Override
    public boolean getDoAfterConnectedOver() {
        return doAfterConnectedOver.get();
    }

    @Override
    public int getAfterConnectCommandTimeoutMill() {
        return DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI;
    }

    @VisibleForTesting
    public boolean getDoAfterConnectedSuccess() {
        return doAfterConnectedSuccess;
    }

}
