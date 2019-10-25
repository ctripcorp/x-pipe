package com.ctrip.xpipe.command;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.netty.TcpPortCheckCommand;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class CausalCommandTest extends AbstractTest {

    @Test
    public void testCausalSucceed() throws ExecutionException, InterruptedException, TimeoutException {
        CausalCommand command = new CausalCommand<String, String>() {
            @Override
            protected void onSuccess(String s) {
                future().setSuccess(s + "suffix");
            }

            @Override
            protected void onFailure(Throwable throwable) {
                future().setFailure(new CausalException("previous failed", throwable));
            }
        };
        CommandFuture<String> future = new DefaultCommandFuture<>();
        CommandFuture commandFuture = command.getCausation(future);
        Assert.assertFalse(commandFuture.isDone());
        future.setSuccess("result");
        waitConditionUntilTimeOut(()->future.isDone(), 10);
        String result = (String) command.future().get();
        Assert.assertTrue(commandFuture.isDone());
        Assert.assertTrue(commandFuture.isSuccess());
        Assert.assertEquals("resultsuffix", result);

    }

    @Test
    public void testCausalFailed() throws ExecutionException, InterruptedException, TimeoutException {
        CausalCommand command = new CausalCommand<String, String>() {
            @Override
            protected void onSuccess(String s) {
                future().setSuccess(s + "suffix");
            }

            @Override
            protected void onFailure(Throwable throwable) {
                future().setFailure(new CausalException("previous failed", throwable));
            }
        };
        CommandFuture<String> future = new DefaultCommandFuture<>();
        CommandFuture commandFuture = command.getCausation(future);
        Assert.assertFalse(commandFuture.isDone());
        future.setFailure(new CausalException("expected behavior"));
        waitConditionUntilTimeOut(()->future.isDone(), 10);
        Assert.assertTrue(commandFuture.isDone());
        Assert.assertNotNull(commandFuture.cause());
        Assert.assertTrue(commandFuture.cause() instanceof CausalException);
    }

    @Test
    public void testCausalCommand() throws Exception {
        Server server = startServer("hello");
        TcpPortCheckCommand tcpPortCheckCommand = new TcpPortCheckCommand("localhost", server.getPort());
        CausalCommand command = new CausalCommand<Boolean, String>() {
            @Override
            protected void onSuccess(Boolean result) {
                future().setSuccess("result: " + result);
            }

            @Override
            protected void onFailure(Throwable throwable) {
                future().setFailure(new CausalException("previous failed", throwable));
            }
        };
        command.getCausation(tcpPortCheckCommand.future());
        tcpPortCheckCommand.execute().await();
        server.stop();
        Assert.assertTrue(command.future().isDone());
        Assert.assertEquals("result: true", command.future().getNow());
    }

    @Test
    public void testCausalCommandFailure() throws Exception {
        TcpPortCheckCommand tcpPortCheckCommand = new TcpPortCheckCommand("localhost", randomPort());
        CausalCommand command = new CausalCommand<Boolean, String>() {
            @Override
            protected void onSuccess(Boolean result) {
                future().setSuccess("result: " + result);
            }

            @Override
            protected void onFailure(Throwable throwable) {
                future().setFailure(new CausalException("previous failed", throwable));
            }
        };
        command.getCausation(tcpPortCheckCommand.future());
        tcpPortCheckCommand.execute().await();
        CommandFuture commandFuture = command.execute();
        Assert.assertTrue(commandFuture.isDone());
        Assert.assertNotNull(commandFuture.cause());
        Assert.assertTrue(commandFuture.cause() instanceof CausalException);
    }

}