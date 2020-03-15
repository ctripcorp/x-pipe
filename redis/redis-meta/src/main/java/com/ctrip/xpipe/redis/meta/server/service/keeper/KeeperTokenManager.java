package com.ctrip.xpipe.redis.meta.server.service.keeper;

import com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService;
import org.springframework.web.bind.annotation.RequestBody;

/**
 * @author chen.zhu
 * <p>
 * Feb 26, 2020
 */
public interface KeeperTokenManager {

    MetaServerKeeperService.KeeperContainerTokenStatusResponse
    refreshKeeperTokenStatus(MetaServerKeeperService.KeeperContainerTokenStatusRequest request);

    boolean closeKeeperRateLimit();
}
