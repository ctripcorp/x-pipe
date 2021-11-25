package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.processor.route;

import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * @author Slight
 * <p>
 * Nov 25, 2021 6:28 PM
 */
@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class BiRouteHealthEventProcessor extends AbstractRouteHealthEventProcessor implements BiDirectionSupport {

    @Override
    protected ProxyTunnelInfo findProxyTunnelInfo(AbstractInstanceEvent instanceSick) {
        return null;
    }

    @Override
    protected long isProbablyHealthyInXSeconds(AbstractInstanceEvent instanceSick) {
        return 0;
    }
}
