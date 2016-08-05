package com.ctrip.xpipe.redis.meta.server.keeper.container;

import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerService;

import org.springframework.web.client.RestTemplate;

import java.util.List;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class DefaultKeeperContainerService implements KeeperContainerService {
    private KeeperContainerMeta keeperContainerMeta;
    private RestTemplate restTemplate;

    public DefaultKeeperContainerService(KeeperContainerMeta keeperContainerMeta, RestTemplate restTemplate) {
        this.keeperContainerMeta = keeperContainerMeta;
        this.restTemplate = restTemplate;
    }

    @Override
    public void addKeeper(KeeperTransMeta keeperTransMeta) {
        restTemplate.postForObject("http://{ip}:{port}/keepers", keeperTransMeta, Void.class, keeperContainerMeta
                .getIp(), keeperContainerMeta.getPort());
    }

    @Override
    public void removeKeeper(KeeperTransMeta keeperTransMeta) {

    }

    @Override
    public void startKeeper(KeeperTransMeta keeperTransMeta) {

    }

    @Override
    public void stopKeeper(KeeperTransMeta keeperTransMeta) {

    }

    @Override
    public List<KeeperInstanceMeta> getAllKeepers() {
        return null;
    }
}
