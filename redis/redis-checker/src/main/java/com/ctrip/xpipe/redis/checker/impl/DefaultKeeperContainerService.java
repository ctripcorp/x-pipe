package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.redis.checker.KeeperContainerService;
import com.ctrip.xpipe.redis.core.entity.KeeperDiskInfo;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;

@Service
public class DefaultKeeperContainerService extends AbstractService implements KeeperContainerService {

    private static final String HTTP_PREFIX = "http://";
    private static final String PATH_GET_KEEPER_DISK_INFO = "/keepers/disk";

    private static final String DEFAULT_KEEPER_CONTAINER_PORT = "8080";

    @Override
    public KeeperDiskInfo getKeeperDiskInfo(String keeperContainerIp) throws RestClientException {
        return restTemplate.exchange(HTTP_PREFIX + keeperContainerIp + ":" + DEFAULT_KEEPER_CONTAINER_PORT + PATH_GET_KEEPER_DISK_INFO,
                HttpMethod.GET, null, KeeperDiskInfo.class).getBody();
    }

}
