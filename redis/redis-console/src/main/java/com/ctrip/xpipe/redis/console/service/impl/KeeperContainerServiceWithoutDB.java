package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cache.TimeBoundCache;
import com.ctrip.xpipe.redis.checker.spring.ConsoleDisableDbCondition;
import com.ctrip.xpipe.redis.checker.spring.DisableDbMode;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.KeeperContainerCreateInfo;
import com.ctrip.xpipe.redis.console.model.KeeperContainerInfoModel;
import com.ctrip.xpipe.redis.console.model.KeeperMsgModel;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.resources.ConsolePortalService;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestOperations;

import java.util.*;
import java.util.stream.IntStream;

@Service
@Conditional(ConsoleDisableDbCondition.class)
@DisableDbMode(true)
public class KeeperContainerServiceWithoutDB implements KeeperContainerService {

    private ConsolePortalService  consolePortalService;

    private ConsoleConfig config;

    private TimeBoundCache<List<KeepercontainerTbl>> allKeepContainer;

    private static RestOperations restTemplate = RestTemplateFactory.createCommonsHttpRestTemplate(10, 20, 3000, 5000);

    @Autowired
    public KeeperContainerServiceWithoutDB(ConsolePortalService consolePortalService, ConsoleConfig config) {
        this.config = config;
        this.consolePortalService = consolePortalService;
        allKeepContainer = new TimeBoundCache<>(config::getCacheRefreshInterval, consolePortalService::getAllKeeperContainers);
    }

    @Override
    public KeepercontainerTbl find(long id) {
        List<KeepercontainerTbl> allKeeper = allKeepContainer.getData();
        for (KeepercontainerTbl keeper : allKeeper) {
            if(keeper.getKeepercontainerId() == id) {
                return keeper;
            }
        }
        return null;
    }

    @Override
    public KeepercontainerTbl find(String ip) {
        List<KeepercontainerTbl> allKeeper = findAll();
        for(KeepercontainerTbl keeperContainer : allKeeper) {
            if(StringUtil.trimEquals(keeperContainer.getKeepercontainerIp(), ip)) {
                return keeperContainer;
            }
        }
        return null;
    }

    @Override
    public List<KeepercontainerTbl> findAll() {
        return allKeepContainer.getData();
    }

    @Override
    public List<KeepercontainerTbl> findAllByDcName(String dcName) {
        List<KeepercontainerTbl> allKeeper = findAll();
        List<KeepercontainerTbl> result = new ArrayList<>();
        for(KeepercontainerTbl keeperContainer : allKeeper) {
            if(StringUtil.trimEquals(keeperContainer.getDcName(), dcName)) {
                result.add(keeperContainer);
            }
        }
        return result;
    }

    @Override
    public List<KeepercontainerTbl> findAllActiveByDcName(String dcName) {
        List<KeepercontainerTbl> allKeeper = findAll();
        List<KeepercontainerTbl> result = new ArrayList<>();
        for(KeepercontainerTbl keeperContainer : allKeeper) {
            if(StringUtil.trimEquals(keeperContainer.getDcName(), dcName) && keeperContainer.isKeepercontainerActive()) {
                result.add(keeperContainer);
            }
        }
        return result;
    }

    @Override
    public List<KeepercontainerTbl> findKeeperCount(String dcName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<KeepercontainerTbl> findBestKeeperContainersByDcCluster(String dcName, String clusterName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<KeepercontainerTbl> findBestKeeperContainersByDcCluster(String dcName, String clusterName, boolean skipAzFilter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<KeepercontainerTbl> getKeeperContainerByAz(Long azId) {
        List<KeepercontainerTbl> result = new ArrayList<>();
        for(KeepercontainerTbl keeperContainer : allKeepContainer.getData()) {
            if(keeperContainer.getAzId() == azId) {
                result.add(keeperContainer);
            }
        }
        return result;
    }

    @Override
    public List<Set<Long>> divideKeeperContainers(int partsCount) {
        List<KeepercontainerTbl> all = findAll();
        if (all == null) return Collections.emptyList();

        List<Set<Long>> result = new ArrayList<>(partsCount);
        IntStream.range(0, partsCount).forEach(i -> result.add(new HashSet<>()));

        all.forEach(keeperContainer -> result.get((int) keeperContainer.getKeepercontainerId() % partsCount)
                .add(keeperContainer.getKeepercontainerId()));

        return result;
    }

    @Override
    public List<KeeperContainerInfoModel> findAllInfos() {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeeperContainerInfoModel findKeeperContainerInfoModelById(long id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<KeeperContainerInfoModel> findAvailableKeeperContainerInfoModelsByDcAzOrgAndTag(String dcName, String azName, String orgName, String tag) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addKeeperContainer(KeeperContainerCreateInfo createInfo) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<KeeperContainerCreateInfo> getDcAllKeeperContainers(String dcName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateKeeperContainer(KeeperContainerCreateInfo createInfo) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteKeeperContainer(String keepercontainerIp, int keepercontainerPort) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addKeeperContainerByInfoModel(KeeperContainerInfoModel keeperContainerInfoModel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateKeeperContainerByInfoModel(KeeperContainerInfoModel keeperContainerInfoModel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, Long> keeperContainerIdDcMap() {
        Map<Long, Long> keeperContainerIdDcMap = new HashMap<>();
        List<KeepercontainerTbl> allKeeperContainers = findAll();
        allKeeperContainers.forEach((keeperContainer) -> {
            keeperContainerIdDcMap.put(keeperContainer.getKeyKeepercontainerId(), keeperContainer.getKeepercontainerDc());
        });
        return keeperContainerIdDcMap;
    }

    @Override
    public List<KeeperMsgModel> getAllKeepers(String keeperIp) {
        throw new UnsupportedOperationException();
    }

}
