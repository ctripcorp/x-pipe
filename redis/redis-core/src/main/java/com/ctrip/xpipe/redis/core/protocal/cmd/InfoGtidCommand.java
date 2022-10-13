package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.dianping.cat.utils.StringUtils;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author ayq
 * <p>
 * 2022/4/13 11:11
 */
public class InfoGtidCommand extends AbstractRedisCommand<GtidSet> {

    public InfoGtidCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
        super(clientPool, scheduled);
    }

    public InfoGtidCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled,
                                  int commandTimeoutMilli) {
        super(clientPool, scheduled, commandTimeoutMilli);
    }

    @Override
    protected GtidSet format(Object payload) {

        String info = payloadToString(payload);
        //filter "all:"
        if (!StringUtils.isEmpty(info) && info.startsWith("# Gtid\r\nall:")) {
            return new GtidSet(info.trim().substring(12));
        }
        return new GtidSet(info);
    }

    @Override
    public ByteBuf getRequest() {
        return new InfoCommand(null, InfoCommand.INFO_TYPE.GTID, null).getRequest();
    }
}
