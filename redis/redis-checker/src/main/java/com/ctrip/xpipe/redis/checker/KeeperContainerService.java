package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.redis.core.entity.KeeperDiskInfo;

public interface KeeperContainerService {

    KeeperDiskInfo getKeeperDiskInfo(String keeperContainerIp);

}
