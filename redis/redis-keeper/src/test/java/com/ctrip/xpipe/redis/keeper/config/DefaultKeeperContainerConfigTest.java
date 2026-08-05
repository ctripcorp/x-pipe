package com.ctrip.xpipe.redis.keeper.config;

import com.ctrip.xpipe.api.config.Config;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultKeeperContainerConfigTest {

    @Mock
    private Config config;

    private DefaultKeeperContainerConfig keeperContainerConfig;

    @Before
    public void setUp() {
        keeperContainerConfig = new DefaultKeeperContainerConfig();
        ReflectionTestUtils.setField(keeperContainerConfig, "config", config);
    }

    @Test
    public void testDefaultModeIsNormal() {
        when(config.get(DefaultKeeperContainerConfig.KEY_MODE, KeeperContainerConfig.MODE_NORMAL))
                .thenReturn(KeeperContainerConfig.MODE_NORMAL);
        Assert.assertEquals(KeeperContainerConfig.MODE_NORMAL, keeperContainerConfig.getMode());
        Assert.assertFalse(keeperContainerConfig.isTfsMode());
    }

    @Test
    public void testTfsModeIgnoreCase() {
        when(config.get(DefaultKeeperContainerConfig.KEY_MODE, KeeperContainerConfig.MODE_NORMAL))
                .thenReturn("tfs");
        Assert.assertEquals(KeeperContainerConfig.MODE_TFS, keeperContainerConfig.getMode());
        Assert.assertTrue(keeperContainerConfig.isTfsMode());
    }

    @Test
    public void testInvalidModeFallbackToNormal() {
        when(config.get(DefaultKeeperContainerConfig.KEY_MODE, KeeperContainerConfig.MODE_NORMAL))
                .thenReturn("INVALID");
        Assert.assertEquals(KeeperContainerConfig.MODE_NORMAL, keeperContainerConfig.getMode());
        Assert.assertFalse(keeperContainerConfig.isTfsMode());
    }
}
