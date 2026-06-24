package com.ctrip.xpipe.redis.meta.server.tfs;

/**
 * TFS gateway RPC for ForceCloseDir (M1: mock implementation only).
 */
public interface TfsGateway {

    void forceCloseDir(String fsId, String dirPath) throws Exception;
}
