package com.ctrip.xpipe.redis.core.protocal;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;

/**
 * @author lishanglin
 * date 2022/2/23
 */
public interface XsyncObserver {

    void onFullSync(GtidSet rdbGtidSet);

    void beginReadRdb(EofType eofType, GtidSet rdbGtidSet);

    void onRdbData(Object rdbData); // TODO: define rdb data format

    void endReadRdb(EofType eofType, GtidSet rdbGtidSet);

    void onContinue();

    void onCommand(Object[] rawCmdArgs);

}
