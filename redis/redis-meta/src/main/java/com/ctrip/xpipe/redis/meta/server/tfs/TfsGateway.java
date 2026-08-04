package com.ctrip.xpipe.redis.meta.server.tfs;

/**
 * TFS gateway RPC for ForceCloseDir.
 * Mock: {@link MockTfsGateway}; Real HTTP: {@link HttpTfsGateway}.
 */
public interface TfsGateway {

    /**
     * @return true if ForceCloseDir succeeded ({@code status.error_code == 0}); false for business failure
     * @throws Exception transport / unexpected errors (HTTP layer, I/O, etc.)
     */
    boolean forceCloseDir(String fsId, String dirPath, String podIp) throws Exception;
}
