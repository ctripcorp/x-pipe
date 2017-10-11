package com.ctrip.xpipe.redis.meta.server.keeper.container;

import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerService;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerServiceFactory;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.google.common.collect.Maps;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;

import java.util.Map;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Component
public class DefaultKeeperContainerServiceFactory implements KeeperContainerServiceFactory {

    private Map<KeeperContainerMeta, KeeperContainerService> services = Maps.newConcurrentMap();
    private RestOperations restTemplate = RestTemplateFactory.createCommonsHttpRestTemplate();

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
