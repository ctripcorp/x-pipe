package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.observer.AbstractObservable;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisServer;
import com.ctrip.xpipe.utils.ChannelUtil;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;

/**
 * @author lishanglin
 * date 2022/6/11
 */
public abstract class AbstractRedisClient<T extends RedisServer> extends AbstractObservable implements RedisClient<T> {

    protected Channel channel;

    protected T redisServer;

    enum COMMAND_STATE{
        READ_SIGN,
        READ_COMMANDS
    }

    private DefaultRedisClient.COMMAND_STATE commandState = DefaultRedisClient.COMMAND_STATE.READ_SIGN;

    private RedisClientProtocol<?>  redisClientProtocol;

    protected abstract Logger getLogger();

    public AbstractRedisClient(Channel channel, T redisServer) {
        this.redisServer = redisServer;
        this.channel = channel;
    }

    @Override
    public T getRedisServer() {
        return redisServer;
    }


    @Override
    public String toString() {
        return ChannelUtil.getDesc(channel);
    }

    @Override
    public String[] readCommands(ByteBuf byteBuf) {

        while(true){

            switch(commandState){
                case READ_SIGN:
                    if(!hasDataRead(byteBuf)){
                        return null;
                    }
                    int readIndex = byteBuf.readerIndex();
                    byte sign = byteBuf.getByte(readIndex);
                    if(sign == RedisClientProtocol.ASTERISK_BYTE){
                        redisClientProtocol = new ArrayParser();
                    }else if(sign == '\n'){
                        byteBuf.readByte();
                        return new String[]{"\n"};
                    }else{
                        redisClientProtocol = new SimpleStringParser();
                    }
                    commandState = DefaultRedisClient.COMMAND_STATE.READ_COMMANDS;
                    break;
                case READ_COMMANDS:
                    RedisClientProtocol<?> resultParser = redisClientProtocol.read(byteBuf);
                    if(resultParser == null){
                        return null;
                    }

                    Object result = resultParser.getPayload();
                    if(result == null){
                        return new String[0];
                    }

                    commandState = DefaultRedisClient.COMMAND_STATE.READ_SIGN ;
                    String []ret = null;
                    if(result instanceof String){
                        ret = handleString((String)result);
                    }else if(result instanceof Object[]){
                        ret = handleArray((Object[])result);
                    }else{
                        throw new IllegalStateException("unkonw result array:" + result);
                    }
                    return ret;
                default:
                    throw new IllegalStateException("unkonwn state:" + commandState);
            }
        }
    }

    private String[] handleArray(Object[] result) {

        String []strArray = new String[result.length];
        int index = 0;
        for(Object param : result){

            if(param instanceof String){
                strArray[index] = (String) param;
            }else if(param instanceof ByteArrayOutputStreamPayload){

                byte [] bytes = ((ByteArrayOutputStreamPayload)param).getBytes();
                strArray[index] = new String(bytes, Codec.defaultCharset);
            }else{
                throw new RedisRuntimeException("request unkonwn, can not be transformed to string!");
            }
            index++;
        }
        return strArray;
    }

    private String[] handleString(String result) {

        String [] args = StringUtil.splitRemoveEmpty("\\s+", result);
        if(args.length == 0){
            logger.info("[handleString][split null]{}", result);
            return null;
        }
        return args;
    }

    private boolean hasDataRead(ByteBuf byteBuf) {

        if(byteBuf.readableBytes() > 0){
            return true;
        }
        return false;
    }

    @Override
    public Channel channel() {
        return channel;
    }

    @Override
    public void sendMessage(ByteBuf byteBuf) {

        channel.writeAndFlush(byteBuf);
    }

    @Override
    public void sendMessage(byte[] bytes) {

        sendMessage(Unpooled.wrappedBuffer(bytes));
    }

    @Override
    public void addChannelCloseReleaseResources(final Releasable releasable){

        channel.closeFuture().addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {

                logger.info("[channel close][release resource]{}", releasable);
                releasable.release();
            }
        });
    }

    @Override
    public void release() throws Exception {
        close();
    }

    @Override
    public String info() {
        return "";
    }

    @Override
    public void close() {
        channel.close();
    }


}
