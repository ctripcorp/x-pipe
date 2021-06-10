package com.ctrip.xpipe.redis.keeper.exception.replication;

/**
 * @author Slight
 * <p>
 * Jun 10, 2021 5:16 PM
 */
public class UnexpectedReplIdException extends KeeperReplicationStoreRuntimeException {

    public UnexpectedReplIdException(String expected, String but) {
        super("expected: " + expected + " but: " + but);
    }
}
