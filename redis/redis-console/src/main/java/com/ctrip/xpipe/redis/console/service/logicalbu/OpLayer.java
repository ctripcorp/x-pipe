package com.ctrip.xpipe.redis.console.service.logicalbu;

/** Which side of the console a keeper-ops failure came from. */
public enum OpLayer {
    /** Console itself: bad input, conflicting operation, business precheck. */
    CONSOLE,
    /** Console → MySQL. */
    CONSOLE_DB,
    /** Console → MetaServer HTTP. */
    META_SERVER,
    /** Console → Keeper / KeeperContainer (TFS recovery). */
    KEEPER
}
