package com.ctrip.xpipe.redis.console.keeper.impl;

import com.ctrip.xpipe.redis.console.keeper.command.CheckKeeperActiveCommand;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;


@RunWith(org.mockito.junit.MockitoJUnitRunner.class)
public class CheckerKeeperActiveCommandTest extends AbstractKeeperCommandTest{

    @Test
    public void checkerKeeperActiveCommandTest() {
        CheckKeeperActiveCommand command = new CheckKeeperActiveCommand(keyedObjectPool, scheduled, key, true);
        command.execute();
        Assert.assertFalse(command.future().isSuccess());
    }


}
