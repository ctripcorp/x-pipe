package com.ctrip.xpipe.redis.console.keeper.impl;

import com.ctrip.xpipe.redis.console.keeper.Command.CheckKeeperActiveCommand;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.when;


@RunWith(org.mockito.junit.MockitoJUnitRunner.class)
public class CheckerKeeperActiveCommandTest extends AbstractKeeperCommandTest{

    @Test
    public void checkerKeeperActiveCommandTest() {
        CheckKeeperActiveCommand command = new CheckKeeperActiveCommand(keyedObjectPool, scheduled, key, true);
        command.execute();
        Assert.assertFalse(command.future().isSuccess());
    }


}
