package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.redis.core.entity.KeeperDiskInfo;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import org.springframework.web.client.RestClientException;

import java.util.List;

public interface KeeperContainerService {

    KeeperDiskInfo getKeeperDiskInfo(String keeperContainerIp) throws RestClientException;

    boolean setKeeperContainerDiskIOLimit(String keeperContainerIp, int keeperContainerPort, int limitInByte);

    List<KeeperInstanceMeta> getAllKeepers(String keeperContainerIp);

    void resetKeeper(String activeKeeperIp, Long replId);

    void releaseRdb(String ip, int port, Long replId);

}
