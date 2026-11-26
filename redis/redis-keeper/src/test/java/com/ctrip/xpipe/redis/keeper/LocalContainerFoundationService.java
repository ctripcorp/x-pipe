package com.ctrip.xpipe.redis.keeper;

import com.ctrip.xpipe.foundation.DefaultFoundationService;

/**
 * Test-only FoundationService: lets several local keepercontainer processes on one machine report different
 * hostnames via {@code -Dxpipe.test.hostname}. Without it they share one hostname, so the disk probe of every
 * process writes and reads the same {@code <store>/disk_check/<hostname>} file and they corrupt each other's
 * probe. Unset: identical to {@link DefaultFoundationService}.
 */
public class LocalContainerFoundationService extends DefaultFoundationService {

    public static final String KEY_HOSTNAME = "xpipe.test.hostname";

    @Override
    public String getHostName() {
        String hostname = System.getProperty(KEY_HOSTNAME);
        return hostname == null || hostname.trim().isEmpty() ? super.getHostName() : hostname.trim();
    }

    @Override
    public int getOrder() {
        // ahead of DefaultFoundationService (LOWEST_PRECEDENCE)
        return LOWEST_PRECEDENCE - 1;
    }
}
