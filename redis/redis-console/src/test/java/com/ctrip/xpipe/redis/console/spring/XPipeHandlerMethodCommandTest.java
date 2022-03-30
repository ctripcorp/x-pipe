package com.ctrip.xpipe.redis.console.spring;

import org.junit.Test;

import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

/**
 * @author Slight
 * <p>
 * Mar 18, 2022 3:23 PM
 */
public class XPipeHandlerMethodCommandTest {

    public int testMethod(int a, int b, int c) {
        return a + b + c;
    }

    @Test
    public void testVarargs1() throws NoSuchMethodException, ExecutionException, InterruptedException {

        Object bean = new XPipeHandlerMethodCommandTest();
        Method method = bean.getClass().getDeclaredMethod("testMethod", int.class, int.class, int.class);

        XPipeHandlerMethodCommand command = new XPipeHandlerMethodCommand(method, bean, 1, 2, 3);
        assertEquals(6, command.execute().get());

        testVarargs2(method, bean, 1, 2, 3);
    }

    public void testVarargs2(Method method, Object bean, Object... args) throws ExecutionException, InterruptedException {

        XPipeHandlerMethodCommand command = new XPipeHandlerMethodCommand(method, bean, args);
        assertEquals(6, command.execute().get());
    }
}