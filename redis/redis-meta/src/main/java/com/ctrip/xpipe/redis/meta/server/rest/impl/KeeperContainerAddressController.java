package com.ctrip.xpipe.redis.meta.server.rest.impl;

import com.ctrip.xpipe.redis.core.keeper.container.CurrentDcKeeperContainerAddressService;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerAddressSnapshot;
import com.ctrip.xpipe.spring.AbstractController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 导出 MetaServer 所在机房的 keepercontainer 地址缓存快照。
 */
@RestController
@RequestMapping(AbstractController.API_PREFIX)
public class KeeperContainerAddressController extends AbstractController {

    @Autowired
    private CurrentDcKeeperContainerAddressService keeperContainerAddressService;

    @GetMapping(KEEPER_CONTAINER_ADDRESS_SNAPSHOT_PATH)
    public KeeperContainerAddressSnapshot getKeeperContainerAddressSnapshot() {
        return keeperContainerAddressService.getAddressSnapshot();
    }
}
