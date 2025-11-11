package com.ctrip.xpipe.redis.core.protocal;

public interface GapAllowedSync extends Psync {
	String PSYNC = "PSYNC";
	String XSYNC = "XSYNC";
	String XFULL_SYNC = "XFULLRESYNC";
	String XPARTIAL_SYNC = "XCONTINUE";
	String UUID_INSTRESTED_DEFAULT = "*";
	String UUID_INSTRESTED_FULL = "?";
	String XSYNC_OPT_MAXGAP = "MAXGAP";
	int DEFAULT_XSYNC_MAXGAP_CROSSREGION = 10000;
	int DEFAULT_XSYNC_MAXGAP = 0;
	String XSYNC_OPT_LOST = "GTID.LOST";

}
