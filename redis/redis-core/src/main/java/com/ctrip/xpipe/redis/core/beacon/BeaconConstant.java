package com.ctrip.xpipe.redis.core.beacon;

import java.sql.Timestamp;

public final class BeaconConstant {

    public static final Timestamp DEFAULT_OPERATING_UNTIL = Timestamp.valueOf("1970-01-01 00:00:00");
    public static final long DEFAULT_OPERATING_UNTIL_MILLIS = DEFAULT_OPERATING_UNTIL.getTime();

}
