package com.ctrip.xpipe.redis.core.protocal;

import com.ctrip.xpipe.gtid.GtidSet;

public interface GapAllowedSyncObserver extends PsyncObserver {
	void onXFullSync(String replId, long replOff, String masterUuid, GtidSet gtidLost);
	void onXContinue(String replId, long replOff, String masterUuid, GtidSet gtidCont);
	void onSwitchToXsync(String replId, long replOff, String masterUuid, GtidSet gtidCont, GtidSet gtidLost);
	void onSwitchToPsync(String replId, long replOff);
	void onUpdateXsync();
}
