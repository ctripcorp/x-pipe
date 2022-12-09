package com.ctrip.xpipe.redis.meta.server.keeper.applier.container;

import com.ctrip.xpipe.redis.core.keeper.applier.container.ApplierContainerService;
import com.ctrip.xpipe.redis.core.keeper.applier.container.ApplierContainerServiceFactory;
import com.ctrip.xpipe.redis.core.entity.ApplierContainerMeta;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.google.common.collect.Maps;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;

import java.util.Map;

/**
 * @author ayq
 * <p>
 * 2022/4/2 12:28
 */
@Component
public class DefaultApplierContainerServiceFactory implements ApplierContainerServiceFactory {

    private Map<ApplierContainerMeta, ApplierContainerService> services = Maps.newConcurrentMap();
    private RestOperations restTemplate = RestTemplateFactory.createCommonsHttpRestTemplate();

    @Override
    public ApplierContainerService getOrCreateApplierContainerService(ApplierContainerMeta applierContainerMeta) {
        if (!services.containsKey(applierContainerMeta)) {
            synchronized (this) {
                if (!services.containsKey(applierContainerMeta)) {
                    services.put(applierContainerMeta, new DefaultApplierContainerService(applierContainerMeta, restTemplate));
                }
            }
        }
        return services.get(applierContainerMeta);
    }
}
