package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.redis.core.entity.KeeperDiskInfo;
import org.springframework.web.client.RestClientException;

public interface KeeperContainerService {

    KeeperDiskInfo getKeeperDiskInfo(String keeperContainerIp) throws RestClientException;

}
