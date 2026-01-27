package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author TB
 * @date 2026/1/26 18:01
 */
public abstract class GtidxAddAndRemoveCommand<T> extends GtidxCommand<T>{
    public GtidxAddAndRemoveCommand(SimpleObjectPool clientPool, ScheduledExecutorService scheduled) {
        super(clientPool, scheduled);
    }

    public GtidxAddAndRemoveCommand(SimpleObjectPool clientPool, String uuid, long startGno, long endNo, ScheduledExecutorService scheduled) {
        super(clientPool, scheduled);
        this.uuid = uuid;
        this.startGno = startGno;
        this.endGno = endNo;
    }

    @Override
    protected String getValue() {
        return " "+uuid+" "+startGno+" "+endGno;
    }

    public static class GtidxAddExecutedCommand extends GtidxAddAndRemoveCommand<Long> {

        public GtidxAddExecutedCommand(SimpleObjectPool clientPool,String uuid,long startGno,long endNo, ScheduledExecutorService scheduled) {
            super(clientPool,uuid,startGno,endNo, scheduled);
        }

        @Override
        protected Long format(Object payload) {
            return payloadToLong(payload);
        }

        @Override
        public ByteBuf getRequest() {
            return new RequestStringParser(CMD, " add executed " +getValue()).format();
        }

    }


    public static class GtidxRemoveLostCommand extends GtidxAddAndRemoveCommand<Long> {

        public GtidxRemoveLostCommand(SimpleObjectPool clientPool,String uuid,long startGno,long endNo, ScheduledExecutorService scheduled) {
            super(clientPool,uuid,startGno,endNo, scheduled);
        }

        @Override
        protected Long format(Object payload) {
            return payloadToLong(payload);
        }

        @Override
        public ByteBuf getRequest() {
            return new RequestStringParser(CMD, " remove lost " +getValue()).format();
        }

    }

}
