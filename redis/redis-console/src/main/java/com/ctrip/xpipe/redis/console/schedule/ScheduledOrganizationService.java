package com.ctrip.xpipe.redis.console.schedule;

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
public class ScheduledOrganizationService {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    OrganizationService organizationService;


    @Scheduled(fixedRate = XPipeConsoleConstant.SCHEDULED_ORGANIZATION_SERVICE)
    public void updateOrganizations() {
        logger.info("[updateOrganizations] update organization table @ {}", DateTimeUtils.currentTimeAsString());
        organizationService.updateOrganizations();
    }
}
