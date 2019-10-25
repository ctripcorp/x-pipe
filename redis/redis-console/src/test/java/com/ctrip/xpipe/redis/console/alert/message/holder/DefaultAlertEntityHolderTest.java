package com.ctrip.xpipe.redis.console.alert.message.holder;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class DefaultAlertEntityHolderTest extends AbstractTest {

    private ALERT_TYPE type = ALERT_TYPE.CLIENT_INCONSIS;

    private DefaultAlertEntityHolder holder;

    @Before
    public void beforeDefaultAlertEntityHolderTest() {
        holder = new DefaultAlertEntityHolder(type);
    }

    private AlertEntity alertEntity() {
        return new AlertEntity(localHostport(randomPort()), randomString(),
                randomString(), randomString(), randomString(), type);
    }

    @Test
    public void getAlertType() {
        Assert.assertEquals(type, holder.getAlertType());
    }

    @Test(expected = IllegalArgumentException.class)
    public void hold() {
        holder.hold(new AlertEntity(null, null, null, null, null, ALERT_TYPE.CLIENT_INSTANCE_NOT_OK));
    }

    @Test
    public void remove() {
        AlertEntity alertEntity = alertEntity();
        holder.hold(alertEntity);
        holder.remove(alertEntity);
        Assert.assertFalse(holder.hasAlerts());
    }

    @Test
    public void removeIf() {
        final int nearlyTheSameTime = 50;
        holder.hold(alertEntity());
        holder.removeIf(alertEntity -> System.currentTimeMillis() - alertEntity.getDate().getTime() <= nearlyTheSameTime);
        Assert.assertFalse(holder.hasAlerts());
    }

    @Test
    public void hasAlerts() {
    }

    @Test
    public void allAlerts() {
        int N = 10;
        for(int i = 0; i < N; i++) {
            holder.hold(alertEntity());
        }
        Assert.assertEquals(N, holder.allAlerts().size());
    }
}