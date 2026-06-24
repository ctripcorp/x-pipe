package com.ctrip.xpipe.redis.meta.server.tfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * M1 mock gateway — always succeeds.
 */
public class MockTfsGateway implements TfsGateway {

    private static final Logger logger = LoggerFactory.getLogger(MockTfsGateway.class);

    @Override
    public void forceCloseDir(String fsId, String dirPath) {
        logger.info("[MockTfsGateway][forceCloseDir]fsId={}, dirPath={}", fsId, dirPath);
    }
}
