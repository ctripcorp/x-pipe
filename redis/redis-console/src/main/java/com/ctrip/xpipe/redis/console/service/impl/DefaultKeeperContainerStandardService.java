package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.KeeperContainerOverloadStandardModel;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import com.ctrip.xpipe.redis.console.service.KeeperContainerStandardService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class DefaultKeeperContainerStandardService implements KeeperContainerStandardService {

    @Autowired
    private ConsoleConfig config;

    @Autowired
    private KeeperContainerService keeperContainerService;

    private static final long DEFAULT_PEER_DATA_OVERLOAD = 474L * 1024 * 1024 * 1024;

    private static final long DEFAULT_KEEPER_FLOW_OVERLOAD = 270 * 1024;

    private static final Logger logger = LoggerFactory.getLogger(DefaultKeeperContainerStandardService.class);

    private static final String currentDc = FoundationService.DEFAULT.getDataCenter();

    @Override
    public void getAndFlushStandard(Map<String, KeeperContainerUsedInfoModel> currentDcAllKeeperContainerUsedInfoModelMap) {
        KeeperContainerOverloadStandardModel keeperContainerOverloadStandard = config.getKeeperContainerOverloadStandards().get(currentDc);
        double loadFactor = config.getKeeperContainerOverloadFactor();
        KeeperContainerOverloadStandardModel defaultOverloadStandard = getDefaultStandard(keeperContainerOverloadStandard, loadFactor);
        logger.info("[analyzeKeeperContainerUsedInfo] keeperContainerDefaultOverloadStandard: {}", defaultOverloadStandard);
        for (KeeperContainerUsedInfoModel infoModel : currentDcAllKeeperContainerUsedInfoModelMap.values()) {
            KeeperContainerOverloadStandardModel realKeeperContainerOverloadStandard = getRealStandard(keeperContainerOverloadStandard, defaultOverloadStandard, infoModel, loadFactor);
            infoModel.setInputFlowStandard(realKeeperContainerOverloadStandard.getFlowOverload());
            infoModel.setRedisUsedMemoryStandard(realKeeperContainerOverloadStandard.getPeerDataOverload());
        }


    }

    private KeeperContainerOverloadStandardModel getDefaultStandard(KeeperContainerOverloadStandardModel keeperContainerOverloadStandard, double loadFactor){
        if (keeperContainerOverloadStandard == null) {
            return new KeeperContainerOverloadStandardModel()
                    .setFlowOverload((long) (DEFAULT_KEEPER_FLOW_OVERLOAD * loadFactor)).setPeerDataOverload((long) (DEFAULT_PEER_DATA_OVERLOAD * loadFactor));
        } else {
            return new KeeperContainerOverloadStandardModel()
                    .setFlowOverload((long) (keeperContainerOverloadStandard.getFlowOverload() * loadFactor)).setPeerDataOverload((long) (keeperContainerOverloadStandard.getFlowOverload() * loadFactor));
        }
    }

    private KeeperContainerOverloadStandardModel getRealStandard(KeeperContainerOverloadStandardModel keeperContainerOverloadStandard,
                                                                 KeeperContainerOverloadStandardModel defaultOverloadStandard,
                                                                 KeeperContainerUsedInfoModel infoModel,
                                                                 double loadFactor){
        KeepercontainerTbl keepercontainerTbl = keeperContainerService.find(infoModel.getKeeperIp());
        infoModel.setDiskType(keepercontainerTbl.getKeepercontainerDiskType());
        infoModel.setKeeperContainerActive(keepercontainerTbl.isKeepercontainerActive());
        if (keeperContainerOverloadStandard != null && keeperContainerOverloadStandard.getDiskTypes() != null && !keeperContainerOverloadStandard.getDiskTypes().isEmpty()) {
            for (KeeperContainerOverloadStandardModel.DiskTypesEnum diskType : keeperContainerOverloadStandard.getDiskTypes()) {
                if (diskType.getDiskType().getDesc().equals(keepercontainerTbl.getKeepercontainerDiskType())) {
                    return new KeeperContainerOverloadStandardModel()
                            .setFlowOverload((long) (diskType.getFlowOverload() * loadFactor))
                            .setPeerDataOverload((long) (diskType.getPeerDataOverload() * loadFactor));

                }
            }
        }
        return defaultOverloadStandard;
    }

}
