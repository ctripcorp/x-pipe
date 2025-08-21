package com.ctrip.xpipe.command;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.netty.TcpPortCheckCommand;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class CausalChainTest extends AbstractTest {

    @Test
    public void testCausalChain() throws Exception {
        Server server = startServer("hello");
        TcpPortCheckCommand tcpPortCheckCommand = new TcpPortCheckCommand("localhost", server.getPort());
        CausalChain chain = new CausalChain();
        chain.add(tcpPortCheckCommand);
        AtomicBoolean finalResult = new AtomicBoolean(false);
        CausalCommand command = new CausalCommand<Boolean, Boolean>() {
            @Override
            protected void onSuccess(Boolean result) {
                finalResult.set(true);
                future().setSuccess(result);
            }

            @Override
            protected void onFailure(Throwable throwable) {
                future().setFailure(throwable);
            }
        };
        chain.add(command);
        CommandFuture future = chain.execute();
        waitConditionUntilTimeOut(()->future.isDone(), 1000);
        server.stop();
        Assert.assertTrue(finalResult.get());
    }

    @Test
    public void testCausalChainFailure() throws Exception {
        TcpPortCheckCommand tcpPortCheckCommand = new TcpPortCheckCommand("localhost", randomPort());
        CausalChain chain = new CausalChain();
        chain.add(tcpPortCheckCommand);
        AtomicBoolean finalResult = new AtomicBoolean(false);
        CausalCommand command = new CausalCommand<Boolean, Boolean>() {
            @Override
            protected void onSuccess(Boolean result) {
                finalResult.set(true);
                future().setSuccess(result);
            }

            @Override
            protected void onFailure(Throwable throwable) {
                future().setFailure(throwable);
            }
        };
        chain.add(command);
        CommandFuture future = chain.execute();
        waitConditionUntilTimeOut(()->future.isDone(), 1000);
        Assert.assertFalse(finalResult.get());
    }

    @Test
    public void testContinousCausalChain() throws InterruptedException, TimeoutException {
        CausalChain causalChain = new CausalChain();
        Command<Integer> initCommand = new AbstractCommand<Integer>() {
            @Override
            protected void doExecute() throws Exception {
                future().setSuccess(0);
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return "InitCommand";
            }
        };
        causalChain.add(initCommand);
        int task = 1000;
        for(int i = 0; i < task; i++) {
            causalChain.add(new Counter());
        }
        CommandFuture future = causalChain.execute(executors);
        waitConditionUntilTimeOut(()->future.isDone(), 3000);
        Assert.assertTrue(future.isSuccess());
        List<CommandFuture<?>> futures = causalChain.getResult();

        Assert.assertEquals(task, futures.get(futures.size()-1).getNow());
    }

    @Test
    public void failBreakTest() throws Exception {
        CausalCommand<String, String> causalCommand = new CausalCommand<String, String>() {
            @Override
            protected void onSuccess(String result) {
                future().setSuccess(result);
            }

            @Override
            protected void onFailure(Throwable throwable) {
                if (throwable instanceof TimeoutException) future().setSuccess();
                else future().setFailure(throwable);
            }
        };

        Command<String> timeoutCommand = new AbstractCommand<String>() {
            @Override
            protected void doExecute() throws Exception {
                future().setFailure(new TimeoutException());
            }

            @Override
            protected void doReset() {
            }

            @Override
            public String getName() {
                return "TimeoutCommand";
            }
        };

        Command<String> otherCommand = new AbstractCommand<String>() {
            @Override
            protected void doExecute() throws Exception {
                future().setFailure(new Exception());
            }

            @Override
            protected void doReset() {
            }

            @Override
            public String getName() {
                return "OtherCommand";
            }
        };


        CausalChain timeoutChain = new CausalChain();
        CausalChain otherChain = new CausalChain();

        timeoutChain.add(timeoutCommand);
        timeoutChain.add(causalCommand);
        CommandFuture timeoutFuture = timeoutChain.execute(executors);
        waitConditionUntilTimeOut(()->timeoutFuture.isDone(), 1000);
        Assert.assertTrue(timeoutFuture.isSuccess());

        causalCommand.reset();
        otherChain.add(otherCommand);
        otherChain.add(causalCommand);
        CommandFuture otherFuture = otherChain.execute(executors);
        waitConditionUntilTimeOut(()->otherFuture.isDone(), 1000);
        Assert.assertFalse(otherFuture.isSuccess());
    }

    private class Counter extends CausalCommand<Integer, Integer> {

        @Override
        protected void onSuccess(Integer integer) {
            future().setSuccess(new Integer(integer+1));
        }

        @Override
        protected void onFailure(Throwable throwable) {
            future().setFailure(throwable);
        }
    }

}