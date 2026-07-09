package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.service.BeaconCheckConfigService;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.utils.DateTimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;

import static com.ctrip.xpipe.redis.core.beacon.BeaconConstant.DEFAULT_OPERATING_UNTIL;

@Service
public class BeaconCheckConfigServiceImpl implements BeaconCheckConfigService {

    private static final Logger logger = LoggerFactory.getLogger(BeaconCheckConfigServiceImpl.class);

    private final DcClusterShardService dcClusterShardService;

    @Autowired
    public BeaconCheckConfigServiceImpl(DcClusterShardService dcClusterShardService) {
        this.dcClusterShardService = dcClusterShardService;
    }

    @Override
    public void stopBeaconCheck(String clusterName, String dc, List<String> shards, int maintainMinutes) throws Exception {
        Date until = DateTimeUtils.getMinutesLaterThan(new Date(), maintainMinutes);
        int affected = dcClusterShardService.batchUpdateOperatingUntil(dc, clusterName, shards, until);
        // 不主动刷新缓存，等待自动刷新生效
        logger.info("[stopBeaconCheck][{}][{}][{}] until {}, affected {}", clusterName, dc, shards, until, affected);
    }

    @Override
    public void startBeaconCheck(String clusterName, String dc, List<String> shards) throws Exception {
        int affected = dcClusterShardService.batchUpdateOperatingUntil(dc, clusterName, shards, DEFAULT_OPERATING_UNTIL);
        // 不主动刷新缓存，等待自动刷新生效
        logger.info("[startBeaconCheck][{}][{}][{}] affected {}", clusterName, dc, shards, affected);
    }
}
