package com.ctrip.xpipe.redis.core.protocal;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import io.netty.buffer.ByteBuf;

/**
 * @author lishanglin
 * date 2022/2/23
 */
public interface XsyncObserver {

    void onFullSync(GtidSet rdbGtidSet, long rdbOffset);

    void beginReadRdb(EofType eofType, GtidSet rdbGtidSet, long rdbOffset);

    void onRdbData(ByteBuf rdbData);

    void endReadRdb(EofType eofType, GtidSet rdbGtidSet, long rdbOffset);

    void onContinue(GtidSet gtidSetExcluded, long continueOffset);

    void onCommand(long commandOffset, Object[] rawCmdArgs);

}
