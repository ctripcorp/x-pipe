package com.ctrip.xpipe.redis.console.keeper.impl;

import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.KeeperContainerOverloadStandardModel;
import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author yu
 * <p>
 * 2023/9/20
 */
@RunWith(org.mockito.junit.MockitoJUnitRunner.class)
public class DefaultKeeperContainerUsedInfoAnalyzerTest {

    @InjectMocks
    private DefaultKeeperContainerUsedInfoAnalyzer analyzer;

    @Mock
    private ConsoleConfig config;

    @Before
    public void before() {
        Mockito.when(config.getClusterDividedParts()).thenReturn(2);
        Map<String, KeeperContainerOverloadStandardModel> standards = Maps.newHashMap();
        standards.put("jq", new KeeperContainerOverloadStandardModel().setFlowOverload(10).setPeerDataOverload(10));
        Mockito.when(config.getKeeperContainerOverloadStandards()).thenReturn(standards);
    }

    @Test
    public void getAllDcReadyToMigrationKeeperContainers() {
        List<KeeperContainerUsedInfoModel> model1 = new ArrayList<>();
        model1.add(new KeeperContainerUsedInfoModel("1.1.1.1", "jq", 10, 10));


    }
}