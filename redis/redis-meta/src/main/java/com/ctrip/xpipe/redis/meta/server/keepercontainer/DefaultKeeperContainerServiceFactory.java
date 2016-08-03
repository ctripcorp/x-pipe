package com.ctrip.xpipe.redis.meta.server.keepercontainer;

import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.keepercontainer.KeeperContainerService;
import com.ctrip.xpipe.redis.core.keepercontainer.KeeperContainerServiceFactory;
import com.google.common.collect.Maps;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Component
public class DefaultKeeperContainerServiceFactory implements KeeperContainerServiceFactory {

    private Map<KeeperContainerMeta, KeeperContainerService> services = Maps.newConcurrentMap();
    private RestTemplate restTemplate = new RestTemplate();

    @Override
    public KeeperContainerService getOrCreateKeeperContainerService(KeeperContainerMeta keeperContainerMeta) {
        if (!services.containsKey(keeperContainerMeta)) {
            synchronized (this) {
                if (!services.containsKey(keeperContainerMeta)) {
                    services.put(keeperContainerMeta, new DefaultKeeperContainerService(keeperContainerMeta, restTemplate));
                }
            }
        }
        return services.get(keeperContainerMeta);
    }
}
