package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.keeper.entity.KeeperContainerDiskType;
import com.ctrip.xpipe.redis.console.model.KeeperContainerOverloadStandardModel;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import com.ctrip.xpipe.redis.console.service.KeeperContainerAnalyzerService;
import com.ctrip.xpipe.redis.console.service.OrganizationService;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

import static com.ctrip.xpipe.redis.console.service.ConfigService.KEY_KEEPER_CONTAINER_STANDARD;

@Component
public class DefaultKeeperContainerAnalyzerService implements KeeperContainerAnalyzerService {

    @Autowired
    private ConfigService configService;

    @Autowired
    private KeeperContainerService keeperContainerService;

    @Autowired
    private OrganizationService organizationService;

    private static final long DEFAULT_PEER_DATA_OVERLOAD = 474L * 1024 * 1024 * 1024;

    private static final long DEFAULT_KEEPER_FLOW_OVERLOAD = 270 * 1024;

    private static final Logger logger = LoggerFactory.getLogger(DefaultKeeperContainerAnalyzerService.class);

    private static final String KEEPER_STANDARD = "keeper_standard";

    @Override
    public void initStandard(Map<String, KeeperContainerUsedInfoModel> currentDcAllKeeperContainerUsedInfoModelMap) {
        Map<String, Long> standardMap = new HashMap<>();
        for (KeeperContainerDiskType value : KeeperContainerDiskType.values()) {
            try {
                standardMap.put(value.getPeerData(), Long.parseLong(configService.getConfig(KEY_KEEPER_CONTAINER_STANDARD, value.getPeerData()).getVal()));
                standardMap.put(value.getInputFlow(), Long.parseLong(configService.getConfig(KEY_KEEPER_CONTAINER_STANDARD, value.getInputFlow()).getVal()));
            } catch (Exception e) {
                logger.error("[analyzeKeeperContainerUsedInfo] get standardMap:{} error: {}", standardMap, e);
            }
        }
        CatEventMonitor.DEFAULT.logEvent(KEEPER_STANDARD, standardMap.toString());
        KeeperContainerOverloadStandardModel defaultOverloadStandard = getDefaultStandard(standardMap);
        for (KeeperContainerUsedInfoModel infoModel : currentDcAllKeeperContainerUsedInfoModelMap.values()) {
            KeeperContainerOverloadStandardModel realKeeperContainerOverloadStandard = getRealStandard(standardMap, defaultOverloadStandard, infoModel);
            infoModel.setInputFlowStandard(realKeeperContainerOverloadStandard.getFlowOverload());
            infoModel.setRedisUsedMemoryStandard(realKeeperContainerOverloadStandard.getPeerDataOverload());
        }
    }

    private KeeperContainerOverloadStandardModel getDefaultStandard(Map<String, Long> standardMap){
        Long defaultPeerDataStandard = standardMap.get(KeeperContainerDiskType.DEFAULT.getPeerData());
        Long defaultInputFlowStandard = standardMap.get(KeeperContainerDiskType.DEFAULT.getInputFlow());
        KeeperContainerOverloadStandardModel defaultOverloadStandard;
        if (defaultInputFlowStandard != null && defaultPeerDataStandard != null) {
            defaultOverloadStandard = new KeeperContainerOverloadStandardModel(defaultPeerDataStandard, defaultInputFlowStandard);
        } else {
            defaultOverloadStandard = new KeeperContainerOverloadStandardModel(DEFAULT_PEER_DATA_OVERLOAD, DEFAULT_KEEPER_FLOW_OVERLOAD);
        }
        logger.info("[analyzeKeeperContainerUsedInfo] keeperContainerDefaultOverloadStandard: {}", defaultOverloadStandard);
        return defaultOverloadStandard;
    }

    private KeeperContainerOverloadStandardModel getRealStandard( Map<String, Long> standardMap,
                                                                 KeeperContainerOverloadStandardModel defaultOverloadStandard,
                                                                 KeeperContainerUsedInfoModel infoModel){
        KeepercontainerTbl keepercontainerTbl = keeperContainerService.find(infoModel.getKeeperIp());
        infoModel.setDiskType(keepercontainerTbl.getKeepercontainerDiskType());
        infoModel.setKeeperContainerActive(keepercontainerTbl.isKeepercontainerActive());
        OrganizationTbl organizationTbl = organizationService.getOrganizationTblByCMSOrganiztionId(keepercontainerTbl.getOrgId());
        if (organizationTbl != null) {
            infoModel.setOrg(organizationTbl.getOrgName());
        }
        for (KeeperContainerDiskType value : KeeperContainerDiskType.values()) {
            if (value.getDesc().equalsIgnoreCase(infoModel.getDiskType())) {
                Long peerDataStandard = standardMap.get(value.getPeerData());
                Long inputFlowStandard = standardMap.get(value.getInputFlow());
                if (peerDataStandard != null && inputFlowStandard != null) {
                    return new KeeperContainerOverloadStandardModel(peerDataStandard, inputFlowStandard);
                }
            }
        }
        return defaultOverloadStandard;
    }

    @VisibleForTesting
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    @VisibleForTesting
    public void setKeeperContainerService(KeeperContainerService keeperContainerService) {
        this.keeperContainerService = keeperContainerService;
    }

    @VisibleForTesting
    public void setOrganizationService(OrganizationService organizationService) {
        this.organizationService = organizationService;
    }

}
