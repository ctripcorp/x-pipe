package com.ctrip.xpipe.redis.console.election;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.console.dao.ConfigDao;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.model.ConfigTbl;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.utils.DateTimeUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.unidal.dal.jdbc.DalException;

import java.util.Date;

@Component
public class CrossDcLeaderElectionAction extends AbstractPeriodicElectionAction {

    public static final String KEY_LEASE_CONFIG = "lease";

    public static final String SUB_KEY_CROSS_DC_LEADER = "CROSS_DC_LEADER";

    public static final long MAX_ELECTION_DELAY_SECOND = 30;

    public static final int ELECTION_INTERVAL_MINUTE = 10;

    private static final int MAX_ELECT_RETRY_TIME = 3;

    @Autowired
    private ConfigDao configDao;

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private ClusterService clusterService;

    private ConfigTbl currentConfig;

    protected void doElect() {
        int retryTime = 0;
        ConfigModel model = buildDefaultConfig();

        while (retryTime < MAX_ELECT_RETRY_TIME) {
            retryTime++;

            try {
                logger.info("[doElect] try to elect self to cross dc leader");
                configDao.updateConfigIdempotent(model,
                        DateTimeUtils.getMinutesLaterThan(new Date(), ELECTION_INTERVAL_MINUTE),
                        currentConfig.getDataChangeLastTime());
            } catch (Exception e) {
                logger.warn("[doElect] elect self fail, {}", e.getMessage());
            }

            try {
                refreshConfig();
                if (isConfigActive()) {
                    logger.info("[doElect] new lease take effect, cross dc leader {}", currentConfig.getValue());
                    return;
                }
            } catch (Exception e) {
                logger.warn("[doElect] refresh config fail, {}", e.getMessage());
            }
        }

        logger.warn("[doElect][fail] retry {} times", retryTime);
    }

    protected boolean shouldElect() {
        try {
            refreshConfig();
        } catch (Exception e) {
            logger.warn("[shouldElect] get master dc lease fail {}", e.getMessage());
        }

        if (null == currentConfig) {
            try {
                configDao.insertConfig(buildDefaultConfig(), new Date(), "lease for cross dc leader");
                refreshConfig();
            } catch (Exception e) {
                logger.warn("[shouldElect] create and get master dc lease fail {}", e.getMessage());
            }
        }

        return isConfigExpired();
    }

    protected void beforeElect() {
        long delay = calculateElectDelay();
        logger.debug("[beforeElect] sleep for {}", delay);
        try {
            if (delay > 0) Thread.sleep(calculateElectDelay());
        } catch (Exception e) {
            logger.warn("[beforeElect] wait for {} fail {}", delay, e.getMessage());
        }
    }

    protected void afterElect() {
        logger.debug("[afterElect] current config {}", currentConfig);
        if (isConfigActive()) notifyObservers(currentConfig.getValue());
    }

    protected long getElectIntervalMillSecond() {
        if (isConfigActive()) return currentConfig.getUntil().getTime() - (new Date()).getTime();
        return ELECTION_INTERVAL_MINUTE * 60 * 1000L;
    }

    private ConfigModel buildDefaultConfig() {
        ConfigModel config = new ConfigModel();
        config.setKey(KEY_LEASE_CONFIG)
                .setSubKey(SUB_KEY_CROSS_DC_LEADER)
                .setVal(FoundationService.DEFAULT.getDataCenter())
                .setUpdateIP(FoundationService.DEFAULT.getLocalIp())
                .setUpdateUser(FoundationService.DEFAULT.getDataCenter() + "-DcLeader");

        return config;
    }

    private long calculateElectDelay() {
        long totalCluster = clusterService.getAllCount();
        long activeClusterCount = countActiveClusterInCurrentDc();
        return (long) ((activeClusterCount / (totalCluster * 1f) ) * MAX_ELECTION_DELAY_SECOND * 1000);
    }

    private long countActiveClusterInCurrentDc() {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        long count = 0;
        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            if (!dcMeta.getId().equalsIgnoreCase(FoundationService.DEFAULT.getDataCenter())) {
                continue;
            }

            for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                if (!clusterMeta.getActiveDc().equals(dcMeta.getId())) {
                    continue;
                }

                count++;
            }
        }

        return count;
    }

    private boolean isConfigExpired() {
        return null != currentConfig && (new Date()).compareTo(currentConfig.getUntil()) >= 0;
    }

    private boolean isConfigActive() {
        return null != currentConfig && (new Date()).compareTo(currentConfig.getUntil()) < 0;
    }

    private void refreshConfig() throws DalException {
        try {
            currentConfig = configDao.getByKeyAndSubId(KEY_LEASE_CONFIG, SUB_KEY_CROSS_DC_LEADER);
        } catch (Exception e) {
            currentConfig = null;
            throw e;
        }
    }
}
