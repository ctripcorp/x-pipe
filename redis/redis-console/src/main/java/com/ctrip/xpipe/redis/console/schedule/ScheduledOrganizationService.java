package com.ctrip.xpipe.redis.console.schedule;

import com.ctrip.xpipe.api.cluster.CrossDcLeaderAware;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.service.OrganizationService;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.DateTimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * @author chen.zhu
 *
 * Sep 04, 2017
 */
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
@Component
public class ScheduledOrganizationService implements CrossDcLeaderAware {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private boolean trigger = false;

    @Autowired
    private OrganizationService organizationService;

    @Override
    public void isCrossDcLeader() {
        trigger = true;
    }

    @Override
    public void notCrossDcLeader() {
        trigger = false;
    }

    @Scheduled(fixedRate = XPipeConsoleConstant.SCHEDULED_ORGANIZATION_SERVICE)
    public void updateOrganizations() {
        if(!trigger) {
            return;
        }
        logger.info("[updateOrganizations] update organization table @ {}", DateTimeUtils.currentTimeAsString());
        // organizationService.updateOrganizations();
    }
}
