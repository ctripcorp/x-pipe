package com.ctrip.xpipe.foundation;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.utils.StringUtil;
import org.junit.Assert;
import org.junit.Test;

public class DefaultFoundationServiceTest extends AbstractTest {

    @Test
    public void testGetHostNameNotEmpty() {
        String hostName = new DefaultFoundationService().getHostName();
        Assert.assertFalse(StringUtil.isEmpty(hostName));
    }
}
