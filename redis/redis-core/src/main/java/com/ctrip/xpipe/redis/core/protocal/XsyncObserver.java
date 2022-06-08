package com.ctrip.xpipe.redis.core.protocal;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import io.netty.buffer.ByteBuf;

/**
 * @author lishanglin
 * date 2022/2/23
 */
public interface XsyncObserver {

    void onFullSync(GtidSet rdbGtidSet);

    void beginReadRdb(EofType eofType, GtidSet rdbGtidSet);

    void onRdbData(ByteBuf rdbData);

    void endReadRdb(EofType eofType, GtidSet rdbGtidSet);

    void onContinue();

    void onCommand(Object[] rawCmdArgs);

}
