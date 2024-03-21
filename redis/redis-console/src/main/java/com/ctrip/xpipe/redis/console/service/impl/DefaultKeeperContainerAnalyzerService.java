package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.keeper.entity.KeeperContainerDiskType;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
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

    @Autowired
    private AzService azService;

    private static final long DEFAULT_PEER_DATA_OVERLOAD = 474L * 1024 * 1024 * 1024;

    private static final long DEFAULT_KEEPER_FLOW_OVERLOAD = 270 * 1024;

    private static final Logger logger = LoggerFactory.getLogger(DefaultKeeperContainerAnalyzerService.class);

    private static final String KEEPER_STANDARD = "keeper_standard";

    @Override
    public void initStandard(List<KeeperContainerUsedInfoModel> currentDcAllKeeperContainerUsedInfoModelMap) {
        Map<String, Long> inputFlowStandardMap = new HashMap<>();
        Map<String, Long> peerDataStandardMap = new HashMap<>();
        List<ConfigModel> configs = configService.getConfigs(KEY_KEEPER_CONTAINER_STANDARD);
        if (configs != null && !configs.isEmpty()) {
            for (ConfigModel config : configs) {
                try {
                    String[] split = config.getSubKey().split(KeeperContainerDiskType.DEFAULT.interval);
                    if (split[1].equalsIgnoreCase(KeeperContainerDiskType.Standard.INPUT_FLOW.getDesc())) {
                        inputFlowStandardMap.put(split[0], Long.parseLong(config.getVal()));
                    } else if (split[1].equalsIgnoreCase(KeeperContainerDiskType.Standard.PEER_DATA.getDesc())) {
                        peerDataStandardMap.put(split[0], Long.parseLong(config.getVal()));
                    }
                } catch (Exception e) {
                    logger.error("[analyzeKeeperContainerUsedInfo] get inputFlowStandardMap:{} peerDataStandardMap:{} error: {}", inputFlowStandardMap, peerDataStandardMap, e);
                }
            }
        }
        CatEventMonitor.DEFAULT.logEvent(KEEPER_STANDARD, inputFlowStandardMap.toString() + inputFlowStandardMap.toString());
        KeeperContainerOverloadStandardModel defaultOverloadStandard = getDefaultStandard(inputFlowStandardMap, peerDataStandardMap);
        for (KeeperContainerUsedInfoModel infoModel : currentDcAllKeeperContainerUsedInfoModelMap) {
            KeeperContainerOverloadStandardModel realKeeperContainerOverloadStandard = getRealStandard(inputFlowStandardMap, peerDataStandardMap, defaultOverloadStandard, infoModel);
            infoModel.setInputFlowStandard(realKeeperContainerOverloadStandard.getFlowOverload());
            infoModel.setRedisUsedMemoryStandard(realKeeperContainerOverloadStandard.getPeerDataOverload());
        }
    }

    private KeeperContainerOverloadStandardModel getDefaultStandard(Map<String, Long> inputFlowStandardMap, Map<String, Long> peerDataStandardMap){
        Long defaultPeerDataStandard = peerDataStandardMap.get(KeeperContainerDiskType.DEFAULT.getDesc());
        Long defaultInputFlowStandard = inputFlowStandardMap.get(KeeperContainerDiskType.DEFAULT.getDesc());
        KeeperContainerOverloadStandardModel defaultOverloadStandard;
        if (defaultInputFlowStandard != null && defaultPeerDataStandard != null) {
            defaultOverloadStandard = new KeeperContainerOverloadStandardModel(defaultPeerDataStandard, defaultInputFlowStandard);
        } else {
            defaultOverloadStandard = new KeeperContainerOverloadStandardModel(DEFAULT_PEER_DATA_OVERLOAD, DEFAULT_KEEPER_FLOW_OVERLOAD);
        }
        logger.info("[analyzeKeeperContainerUsedInfo] keeperContainerDefaultOverloadStandard: {}", defaultOverloadStandard);
        return defaultOverloadStandard;
    }

    private KeeperContainerOverloadStandardModel getRealStandard( Map<String, Long> inputFlowStandardMap,
                                                                  Map<String, Long> peerDataStandardMap,
                                                                 KeeperContainerOverloadStandardModel defaultOverloadStandard,
                                                                 KeeperContainerUsedInfoModel infoModel){
        KeepercontainerTbl keepercontainerTbl = keeperContainerService.find(infoModel.getKeeperIp());
        infoModel.setDiskType(keepercontainerTbl.getKeepercontainerDiskType());
        infoModel.setKeeperContainerActive(keepercontainerTbl.isKeepercontainerActive());
        AzTbl availableZoneTblById = azService.getAvailableZoneTblById(keepercontainerTbl.getAzId());
        if (availableZoneTblById != null) {
            infoModel.setAz(availableZoneTblById.getAzName());
        }
        OrganizationTbl organizationTbl = organizationService.getOrganizationTblByCMSOrganiztionId(keepercontainerTbl.getOrgId());
        if (organizationTbl != null) {
            infoModel.setOrg(organizationTbl.getOrgName());
        }
        Long inputFlowStandard = null;
        Long peerDataStandard = null;
        for (Map.Entry<String, Long> entry : inputFlowStandardMap.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(infoModel.getDiskType())) {
                inputFlowStandard = entry.getValue();
            }
        }
        for (Map.Entry<String, Long> entry : peerDataStandardMap.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(infoModel.getDiskType())) {
                peerDataStandard = entry.getValue();
            }
        }

        if (peerDataStandard != null && inputFlowStandard != null) {
            return new KeeperContainerOverloadStandardModel(peerDataStandard, inputFlowStandard);
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

    @VisibleForTesting
    public void setAzService(AzService azService) {
        this.azService = azService;
    }
}
