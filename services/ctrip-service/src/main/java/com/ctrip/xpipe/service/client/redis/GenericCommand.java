package com.ctrip.xpipe.service.client.redis;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.ReferenceCountUtil;
import qunar.tc.qclient.redis.codec.Codec;
import qunar.tc.qclient.redis.codec.SedisCodec;
import qunar.tc.qclient.redis.command.Command;
import qunar.tc.qclient.redis.command.RedisResult;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static qunar.tc.qclient.redis.codec.RedisCodec.CRLF;

/**
 * @author Slight
 * <p>
 * Feb 26, 2022 5:39 PM
 */
public class GenericCommand extends Command {

    private final List<Object> args;
    private final Codec codec;

    public GenericCommand(Codec codec) {
        super(null);
        this.codec = codec;
        this.args = new ArrayList<>();
    }

    @Override
    protected void doWrite(Object arg) {
        args.add(arg);
    }

    @Override
    protected void writeToChannel(Channel channel, RedisResult<?> command, Deque<RedisResult<?>> sents) {
        ByteBuf buffer = null;
        try {
            if (argCount <= 0) {
                buffer = channel.alloc().buffer(32);
            } else {
                buffer = channel.alloc().buffer();
            }

            writeHead(buffer);

            if (argCount <= 0) {
                channel.write(buffer);
                return;
            }

            for (int i = 0; i < args.size(); ++i) {
                Object value = args.set(i, null);
                codec.encode(buffer, value);
            }
        } catch (Exception e) {
            command.fail(e);
            if (buffer != null) {
                ReferenceCountUtil.release(buffer);
            }
            return;
        }
        sents.add(command);
        channel.write(buffer);
    }

    @Override
    protected void writeHead(ByteBuf head) {
        if (commandType != null) {
            super.writeHead(head);
        } else {
            head.writeByte('*');
            writeInt(head, this.argCount);
            head.writeBytes(CRLF);
        }
    }
}
