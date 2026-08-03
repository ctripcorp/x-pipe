package com.ctrip.xpipe.redis.meta.server.tfs;

/**
 * TFS gateway RPC for ForceCloseDir.
 * Phase 9: Mock + Unimplemented placeholder; Phase 10: real HTTP.
 */
public interface TfsGateway {

    void forceCloseDir(String fsId, String dirPath, String podIp) throws Exception;
}
