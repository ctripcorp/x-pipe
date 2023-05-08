package com.ctrip.xpipe.redis.console.healthcheck.nonredis.metacache;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.console.AbstractSiteLeaderIntervalAction;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class MetaCacheCheck extends AbstractSiteLeaderIntervalAction {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private AlertManager alertManager;

    private final List<ALERT_TYPE> alertType = Lists.newArrayList(ALERT_TYPE.META_CACHE_BLOCKED);

    private final int META_CACHE_UPDATE_THREASHOLD = 10 * 1000;

    @Override
    protected void doAction() {
        if (isMetaCacheOverDue()) {
            alertManager.alert(null, null, null, ALERT_TYPE.META_CACHE_BLOCKED, "meta-cache-not-update-for-long-time");
        }
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return alertType;
    }

    private boolean isMetaCacheOverDue() {
        return System.currentTimeMillis() - metaCache.getLastUpdateTime() > META_CACHE_UPDATE_THREASHOLD;
    }

    @VisibleForTesting
    protected MetaCacheCheck setMetaCache(MetaCache metaCache) {
        this.metaCache = metaCache;
        return this;
    }

    @VisibleForTesting
    protected MetaCacheCheck setAlertManager(AlertManager alertManager) {
        this.alertManager = alertManager;
        return this;
    }
}
