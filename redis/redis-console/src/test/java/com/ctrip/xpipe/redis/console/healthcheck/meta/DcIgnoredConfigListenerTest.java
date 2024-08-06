package com.ctrip.xpipe.redis.console.healthcheck.meta;

import com.ctrip.xpipe.redis.checker.healthcheck.meta.MetaChangeManager;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;

import static com.ctrip.xpipe.redis.checker.config.impl.CheckConfigBean.KEY_IGNORED_DC_FOR_HEALTH_CHECK;
import static org.mockito.Matchers.anyString;
import static org.mockito.internal.verification.VerificationModeFactory.times;

/**
 * @author chen.zhu
 * <p>
 * Sep 13, 2018
 */
public class DcIgnoredConfigListenerTest {

    @InjectMocks
    private DcIgnoredConfigChangeListener listener = new DcIgnoredConfigChangeListener();

    @Mock
    private MetaChangeManager metaChangeManager;

    @Before
    public void beforeDcIgnoredConfigListenerTest() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testConvert() {
        String[] result = listener.convert("FRA-AWS,UAT,FWS");
        Assert.assertTrue(Arrays.deepEquals(new String[]{"FRA-AWS", "UAT", "FWS"}, result));
    }

    @Test
    public void testDoOnChange() {
        listener.doOnChange(new String[]{"FRA-AWS"}, new String[]{"NULL"});
        Mockito.verify(metaChangeManager, times(1)).startIfPossible("FRA-AWS");
        Mockito.verify(metaChangeManager, times(1)).ignore("NULL");
    }

    @Test
    public void testDoOnChangeWithUpdateOnly() {
        listener.doOnChange(new String[]{}, new String[]{"FRA-AWS"});
        Mockito.verify(metaChangeManager, times(1)).ignore("FRA-AWS");
        Mockito.verify(metaChangeManager, Mockito.never()).startIfPossible(anyString());
    }

    @Test
    public void testDoOnChangeWithRemoveOldOnly() {
        listener.doOnChange(new String[]{"FRA-AWS"}, new String[]{});
        Mockito.verify(metaChangeManager, Mockito.never()).ignore(anyString());
        Mockito.verify(metaChangeManager, times(1)).startIfPossible(anyString());
    }

    @Test
    public void testSupportsKeys() {
        Assert.assertEquals(Lists.newArrayList(KEY_IGNORED_DC_FOR_HEALTH_CHECK), listener.supportsKeys());
    }
}