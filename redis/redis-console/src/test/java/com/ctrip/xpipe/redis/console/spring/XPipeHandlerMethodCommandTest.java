package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.api.sso.UserInfoHolder;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

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

        XPipeHandlerMethodCommand command = new XPipeHandlerMethodCommand(method, bean, UserInfoHolder.DEFAULT,1, 2, 3);
        assertEquals(6, command.execute().get());

        testVarargs2(method, bean, 1, 2, 3);
    }

    public void testVarargs2(Method method, Object bean, Object... args) throws ExecutionException, InterruptedException {

        XPipeHandlerMethodCommand command = new XPipeHandlerMethodCommand(method, bean, UserInfoHolder.DEFAULT, args);
        assertEquals(6, command.execute().get());
    }

    public int returnZero() {
        return 0;
    }

    @Test
    public void testContext() throws NoSuchMethodException, ExecutionException, InterruptedException {

        UserInfoHolder holder = spy(UserInfoHolder.DEFAULT);
        Object expected = new Object();
        when(holder.getContext()).thenReturn(expected);

        Object bean = new XPipeHandlerMethodCommandTest();
        Method method = bean.getClass().getDeclaredMethod("returnZero");

        XPipeHandlerMethodCommand command = new XPipeHandlerMethodCommand(method, bean, holder);
        command.execute().get();

        verify(holder, times(1)).getContext();
        verify(holder, times(1)).setContext(same(expected));
    }
}