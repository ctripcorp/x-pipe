package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cache.TimeBoundCache;
import com.ctrip.xpipe.redis.checker.spring.ConsoleDisableDbCondition;
import com.ctrip.xpipe.redis.checker.spring.DisableDbMode;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.LogicalBuModel;
import com.ctrip.xpipe.redis.console.resources.ConsolePortalService;
import com.ctrip.xpipe.redis.console.service.LogicalBuService;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Service
@Conditional(ConsoleDisableDbCondition.class)
@DisableDbMode(true)
public class LogicalBuServiceWithoutDB implements LogicalBuService {

    private static final Logger logger = LoggerFactory.getLogger(LogicalBuServiceWithoutDB.class);

    @Autowired
    private ConsolePortalService consolePortalService;

    @Autowired
    private ConsoleConfig config;

    private TimeBoundCache<List<LogicalBuModel>> allLogicalBus;

    /**
     * Last successfully loaded value; used when Console is unreachable (same pattern as
     * {@link com.ctrip.xpipe.redis.console.resources.CheckerPersistenceCache}).
     */
    private final CachedValue<List<LogicalBuModel>> lastSuccessfulLogicalBus = new CachedValue<>();

    @PostConstruct
    public void init() {
        allLogicalBus = new TimeBoundCache<>(config::getCacheRefreshInterval, this::loadAll);
    }

    private List<LogicalBuModel> loadAll() {
        try {
            List<LogicalBuModel> all = consolePortalService.getAllLogicalBus();
            if (all == null) {
                logger.warn("[loadAll] portal returned null, keep last value");
            } else {
                lastSuccessfulLogicalBus.update(all);
                return all;
            }
        } catch (RestClientException e) {
            logger.warn("[loadAll] rest fail, {}", e.getMessage());
        } catch (Throwable th) {
            logger.warn("[loadAll] fail", th);
        }
        return lastSuccessfulLogicalBus.getOrElse(Collections.emptyList());
    }

    @Override
    public List<LogicalBuModel> findAll() {
        return allLogicalBus.getData();
    }

    @Override
    public LogicalBuModel findById(long id) {
        for (LogicalBuModel model : allLogicalBus.getData()) {
            if (model.getId() == id) {
                return model;
            }
        }
        throw new BadRequestException("Logical BU not found: " + id);
    }

    @Override
    public LogicalBuModel create(LogicalBuModel model) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LogicalBuModel update(long id, LogicalBuModel model) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(long id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long resolveLogicalBuIdForCluster(String clusterName, long clusterOrgId) {
        if (clusterOrgId <= 0 || StringUtil.isEmpty(clusterName)) {
            return 0L;
        }
        List<LogicalBuModel> candidates = new ArrayList<>();
        for (LogicalBuModel model : allLogicalBus.getData()) {
            if (!model.isActive()) {
                continue;
            }
            List<Long> cmsOrgIds = model.getCmsOrgIds();
            if (cmsOrgIds == null || !cmsOrgIds.contains(clusterOrgId)) {
                continue;
            }
            candidates.add(model);
        }
        if (candidates.isEmpty()) {
            return 0L;
        }
        int idx = Math.floorMod(clusterName.hashCode(), candidates.size());
        return candidates.get(idx).getId();
    }

    private static class CachedValue<T> {
        private volatile boolean initialized;
        private volatile T value;

        void update(T newValue) {
            this.value = newValue;
            this.initialized = true;
        }

        T getOrElse(T defaultValue) {
            return initialized ? value : defaultValue;
        }
    }
}
