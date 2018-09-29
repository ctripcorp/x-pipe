package com.ctrip.xpipe.redis.console;

import org.junit.Test;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Sep 30, 2018
 */
public class MockitoTest {

    @Test
    public void test() {
        Foo foo = new Foo();
        foo = spy(foo);
        when(foo.output()).thenReturn("World");

        System.out.println(foo.output());
    }


    class Foo {
        public String output() {
            return "Hello";
        }
    }
}
