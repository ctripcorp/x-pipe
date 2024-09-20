package com.ctrip.xpipe.redis.console.election;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.dao.ConfigDao;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.model.ConfigTbl;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.DateTimeUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.unidal.dal.jdbc.DalException;

import java.util.Date;

@Component
public class CrossDcLeaderElectionAction extends AbstractPeriodicElectionAction {

    public static final String KEY_LEASE_CONFIG = "LEASE";

    protected static int MAX_ELECTION_DELAY_MILLISECOND = 30 * 1000;

    protected static int ELECTION_INTERVAL_SECOND = 10 * 60;

    protected static int MAX_ELECT_RETRY_TIME = 3;

    protected String dataCenter = FoundationService.DEFAULT.getDataCenter();

    protected String localIp = FoundationService.DEFAULT.getLocalIp();

    private ConfigDao configDao;

    private MetaCache metaCache;

    private String leaseName;

    private ConfigTbl lease;

    private ConsoleConfig config;

    @Autowired
    public CrossDcLeaderElectionAction(ConfigDao configDao, MetaCache metaCache, ConsoleConfig consoleConfig) {
        this.configDao = configDao;
        this.metaCache = metaCache;
        this.leaseName = consoleConfig.getCrossDcLeaderLeaseName();
        this.config = consoleConfig;
    }

    @Override
    protected void doElect() {
        int retryTime = 0;
        ConfigModel model = buildDefaultConfig();

        while (retryTime < MAX_ELECT_RETRY_TIME) {
            retryTime++;

            try {
                logger.debug("[doElect] dc {} try to elect self to cross dc leader", dataCenter);
                configDao.updateConfigIdempotent(model,
                        DateTimeUtils.getSecondsLaterThan(new Date(), ELECTION_INTERVAL_SECOND),
                        lease.getDataChangeLastTime());
            } catch (Exception e) {
                logger.info("[doElect] elect self fail, {}", e.getMessage());
            }

            try {
                refreshConfig();
                if (isLeaseOn()) {
                    logger.info("[doElect] new lease take effect, cross dc leader {}", lease.getValue());
                    return;
                }
            } catch (Exception e) {
                logger.info("[doElect] refresh config fail, {}", e.getMessage());
            }
        }

        logger.info("[doElect][fail] retry {} times", retryTime);
    }

    @Override
    protected boolean shouldElect() {
        if(config.disableDb()) {
            return false;
        }
        try {
            refreshConfig();
        } catch (Exception e) {
            logger.info("[shouldElect] get master dc lease fail {}", e.getMessage());
        }

        if (null == lease) {
            try {
                configDao.insertConfig(buildDefaultConfig(), new Date(), "lease for cross dc leader");
                refreshConfig();
            } catch (Exception e) {
                logger.info("[shouldElect] create and get master dc lease fail {}", e.getMessage());
            }
        }

        return isLeaseExpired();
    }

    @Override
    protected void beforeElect() {
        long delay = calculateElectDelay();
        logger.debug("[beforeElect] sleep for {}", delay);
        try {
            if (delay > 0) Thread.sleep(delay);
        } catch (Exception e) {
            logger.info("[beforeElect] wait for {} fail {}", delay, e.getMessage());
        }
    }

    @Override
    protected void afterElect() {
        logger.debug("[afterElect] current lease {}", lease);
        if (isLeaseOn()) notifyObservers(lease.getValue());
        else if (isLeaseExpired()) notifyObservers(null);
    }

    @Override
    protected long getElectIntervalMillSecond() {
        if (isLeaseOn()) return lease.getUntil().getTime() - (new Date()).getTime();
        else if (isLeaseExpired()) return 0;
        else return ELECTION_INTERVAL_SECOND * 1000L;
    }

    @Override
    protected String getElectionName() {
        return "CrossDcLeaderElection";
    }

    private ConfigModel buildDefaultConfig() {
        ConfigModel config = new ConfigModel();
        config.setKey(KEY_LEASE_CONFIG)
                .setSubKey(leaseName)
                .setVal(dataCenter)
                .setUpdateIP(localIp)
                .setUpdateUser(dataCenter + "-DcLeader");

        return config;
    }

    // make dc leader with least active cluster sleep less
    // so it can be elected to cross dc leader more likely
    private long calculateElectDelay() {
        return (long) (calculateActiveClusterRatio() * MAX_ELECTION_DELAY_MILLISECOND);
    }

    private float calculateActiveClusterRatio() {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (null == xpipeMeta) return 1L;

        long count;
        long totalCluster = 0;
        long activeClusterCount = 0;

        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            count = 0;
            for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
                if (clusterType.supportSingleActiveDC() && !clusterMeta.getActiveDc().equals(dcMeta.getId())) {
                    continue;
                }

                count++;
            }

            if (dcMeta.getId().equalsIgnoreCase(dataCenter)) {
                activeClusterCount += count;
            }
            totalCluster += count;
        }

        if (0 == totalCluster) return 0;
        return activeClusterCount / (totalCluster * 1f);
    }

    private boolean isLeaseExpired() {
        return null != lease && (new Date()).compareTo(lease.getUntil()) >= 0;
    }

    private boolean isLeaseOn() {
        return null != lease && (new Date()).compareTo(lease.getUntil()) < 0;
    }

    private void refreshConfig() throws DalException {
        try {
            lease = configDao.getByKeyAndSubId(KEY_LEASE_CONFIG, leaseName);
        } catch (Exception e) {
            lease = null;
            throw e;
        }
    }

}
